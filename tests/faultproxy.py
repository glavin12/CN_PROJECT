#!/usr/bin/env python3
"""
faultproxy.py - TCP proxy that breaks connections at random byte positions.

IT305 Socket Programming Project, Group 006 + 043.

The server's own fault injection (DEVIATIONS 9) always cuts *inside a data
chunk*. The guidelines ask for correctness when a disconnection happens "at
any point in time", so this proxy can also cut in the middle of a 16-byte
header, a FILE_HDR, a RESUME_OK, a MANIFEST, ... wherever the random byte
falls. Put it between a client and a server (with p = 0):

    client --> 127.0.0.1:<listen> [faultproxy] --> <target host:port> (server)

    python3 tests/faultproxy.py --listen PORT --target HOST:PORT
            [--cut-prob P] [--window BYTES] [--rate BYTES_PER_SEC]
            [--seed N] [--max-cuts N] [--bind ADDR] [--verbose]

  --cut-prob P   for each new connection, with probability P choose a cut
                 point uniformly in [1, --window] bytes of server->client
                 traffic (default 0 = never cut). When exactly that many bytes
                 have been forwarded (and delivered to the client's kernel),
                 both sockets are closed abruptly (RST, SO_LINGER 0).
  --window N     cut window in bytes (default 262144)
  --rate R       throttle server->client forwarding to R bytes/s in total over
                 all connections (a slow link), so a test can kill a client in
                 the middle of a transfer. Default: unlimited.
  --seed N       seed of the cut decisions (default: random, printed on stderr)
  --max-cuts N   stop cutting after N cuts so a test can finish (default: no limit)

Prints "READY" on stdout once it listens (tests wait for that line). Every cut
is logged on stderr as
    faultproxy: CUT conn=<n> at s2c byte <pos>
and a SUMMARY line is printed on SIGTERM / SIGINT.

Only the Python standard library is used (asyncio: one event loop handles
hundreds of connections without one thread per socket).
"""

import argparse
import asyncio
import fcntl
import random
import signal
import socket
import struct
import sys
import termios


def log(msg):
    print("faultproxy: " + msg, file=sys.stderr, flush=True)


class Proxy:
    def __init__(self, args):
        self.args = args
        self.host, port = args.target.rsplit(":", 1)
        self.port = int(port)
        self.rng = random.Random(args.seed)
        self.conns = 0              # connections accepted
        self.cuts = 0               # cuts actually performed
        self.refused = 0            # upstream connect failures
        self.s2c_bytes = 0          # server -> client bytes forwarded (all connections)
        self.c2s_bytes = 0
        self.next_free = 0.0        # rate limiter: time at which the "link" is free again
        # asyncio only keeps WEAK references to tasks, and a StreamReaderProtocol
        # only a weak one to its reader: a connection waiting for data would be an
        # unreachable cycle the garbage collector may destroy mid-transfer. Every
        # running task is therefore kept here until it ends.
        self.live = set()
        self.chunk = 65536
        if args.rate:
            # small reads when throttled, so pacing is smooth (about 100 steps per second)
            self.chunk = max(512, min(65536, args.rate // 100))

    def cut_budget_left(self):
        return self.args.max_cuts is None or self.cuts < self.args.max_cuts

    async def pace(self, n):
        """Global token-bucket-like pacing: n more bytes may go out at `rate` B/s."""
        if not self.args.rate:
            return
        loop = asyncio.get_running_loop()
        now = loop.time()
        start = max(now, self.next_free)
        self.next_free = start + n / self.args.rate
        delay = self.next_free - now
        if delay > 0:
            await asyncio.sleep(delay)

    async def handle(self, c_reader, c_writer):
        me = asyncio.current_task()
        self.live.add(me)
        self.conns += 1
        conn = Conn(self, self.conns, c_reader, c_writer)
        # The cut decision is drawn here, in accept order, from one seeded RNG.
        if self.args.cut_prob > 0 and self.cut_budget_left():
            if self.rng.random() < self.args.cut_prob:
                conn.cut_at = self.rng.randint(1, self.args.window)
        try:
            await conn.run()
        finally:
            self.live.discard(me)


class Conn:
    def __init__(self, proxy, no, c_reader, c_writer):
        self.p = proxy
        self.no = no
        self.c_reader, self.c_writer = c_reader, c_writer
        self.u_reader = self.u_writer = None
        self.cut_at = None          # cut after this many server->client bytes (None = never)
        self.s2c = 0                # server -> client bytes forwarded on this connection
        self.aborted = False

    def vlog(self, msg):
        if self.p.args.verbose:
            log("conn=%d %s" % (self.no, msg))

    async def run(self):
        peer = self.c_writer.get_extra_info("peername")
        self.vlog("open from %s, cut_at=%s" % (peer, self.cut_at))
        try:
            self.u_reader, self.u_writer = await asyncio.wait_for(
                asyncio.open_connection(self.p.host, self.p.port), timeout=10)
        except (OSError, asyncio.TimeoutError) as e:
            self.p.refused += 1
            log("conn=%d upstream connect failed: %s" % (self.no, e))
            self.abort()
            return
        for w in (self.c_writer, self.u_writer):
            sock = w.get_extra_info("socket")
            if sock is not None:
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        pumps = [asyncio.create_task(self.pump_c2s()), asyncio.create_task(self.pump_s2c())]
        self.p.live.update(pumps)
        try:
            await asyncio.gather(*pumps)
        finally:
            self.p.live.difference_update(pumps)
        if not self.aborted:
            for w in (self.c_writer, self.u_writer):
                w.close()
        self.vlog("closed, s2c=%d bytes" % self.s2c)

    def abort(self):
        """Close both sockets at once with an RST (SO_LINGER {1, 0})."""
        if self.aborted:
            return
        self.aborted = True
        for w in (self.c_writer, self.u_writer):
            if w is None:
                continue
            sock = w.get_extra_info("socket")
            try:
                if sock is not None:
                    sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
            except OSError:
                pass
            w.transport.abort()

    async def pump_c2s(self):
        """client -> server: forward everything (requests are small)."""
        try:
            while not self.aborted:
                data = await self.c_reader.read(65536)
                if not data:
                    break
                self.u_writer.write(data)
                await self.u_writer.drain()
                self.p.c2s_bytes += len(data)
            if not self.aborted and self.u_writer.can_write_eof():
                self.u_writer.write_eof()           # pass the client's FIN on
        except (OSError, ConnectionError, RuntimeError):
            self.abort()

    async def pump_s2c(self):
        """server -> client: forward with optional throttling and the planned cut."""
        try:
            while not self.aborted:
                data = await self.u_reader.read(self.p.chunk)
                if not data:
                    break
                if self.cut_at is not None:
                    left = self.cut_at - self.s2c      # always >= 1 here
                    if len(data) >= left:
                        if self.p.cut_budget_left():
                            await self.cut(data[:left])
                            return
                        self.cut_at = None             # --max-cuts reached: just forward
                await self.p.pace(len(data))
                self.c_writer.write(data)
                await self.c_writer.drain()
                self.s2c += len(data)
                self.p.s2c_bytes += len(data)
            if not self.aborted and self.c_writer.can_write_eof():
                self.c_writer.write_eof()           # pass the server's FIN on
        except (OSError, ConnectionError, RuntimeError):
            self.abort()

    async def cut(self, last):
        """Forward the final `last` bytes, wait until the client's kernel has
        them, then reset both connections."""
        self.p.cuts += 1
        await self.p.pace(len(last))
        self.c_writer.write(last)
        self.s2c += len(last)
        self.p.s2c_bytes += len(last)
        # drain() with a zero high-water mark waits until asyncio's buffer is empty.
        self.c_writer.transport.set_write_buffer_limits(high=0)
        await self.c_writer.drain()
        # Then wait (max 0.5 s) until the kernel send queue is empty, i.e. the
        # client's TCP stack acknowledged every byte. Without this the RST could
        # discard bytes still queued here and the cut would land earlier.
        sock = self.c_writer.get_extra_info("socket")
        loop = asyncio.get_running_loop()
        deadline = loop.time() + 0.5
        while sock is not None and loop.time() < deadline:
            try:
                outq = struct.unpack("i", fcntl.ioctl(sock.fileno(), termios.TIOCOUTQ, b"\0" * 4))[0]
            except OSError:
                break
            if outq == 0:
                break
            await asyncio.sleep(0.005)
        log("CUT conn=%d at s2c byte %d" % (self.no, self.s2c))
        self.abort()


async def main_async(args):
    proxy = Proxy(args)
    server = await asyncio.start_server(proxy.handle, args.bind, args.listen,
                                        reuse_address=True, backlog=1024)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)
    log("listening on %s:%d -> %s:%d seed=%d cut_prob=%g window=%d rate=%s max_cuts=%s" % (
        args.bind, args.listen, proxy.host, proxy.port, args.seed, args.cut_prob,
        args.window, args.rate or "unlimited",
        "unlimited" if args.max_cuts is None else args.max_cuts))
    print("READY", flush=True)
    await stop.wait()
    server.close()
    log("SUMMARY conns=%d cuts=%d refused=%d s2c_bytes=%d c2s_bytes=%d" % (
        proxy.conns, proxy.cuts, proxy.refused, proxy.s2c_bytes, proxy.c2s_bytes))


def main():
    ap = argparse.ArgumentParser(description="TCP proxy that cuts connections at random bytes.")
    ap.add_argument("--listen", type=int, required=True, help="port to listen on")
    ap.add_argument("--target", required=True, help="HOST:PORT of the real server")
    ap.add_argument("--bind", default="127.0.0.1", help="listen address (default 127.0.0.1)")
    ap.add_argument("--cut-prob", type=float, default=0.0)
    ap.add_argument("--window", type=int, default=262144)
    ap.add_argument("--rate", type=int, default=0, help="bytes/s server->client (0 = unlimited)")
    ap.add_argument("--seed", type=int, default=None)
    ap.add_argument("--max-cuts", type=int, default=None)
    ap.add_argument("--verbose", "-v", action="store_true", help="log every connection")
    args = ap.parse_args()
    if not 0.0 <= args.cut_prob <= 1.0:
        ap.error("--cut-prob must be in [0, 1]")
    if args.window < 1:
        ap.error("--window must be >= 1")
    if args.rate < 0:
        ap.error("--rate must be >= 0")
    if ":" not in args.target:
        ap.error("--target must be HOST:PORT")
    if args.seed is None:
        args.seed = random.SystemRandom().randrange(1 << 31)
    try:
        asyncio.run(main_async(args))
    except OSError as e:
        log("error: %s" % e)
        sys.exit(1)


if __name__ == "__main__":
    main()
