/* test_util.c - unit tests for util.c */
#include "unit.h"
#include "fds.h"
#include "util.h"

#include <stdlib.h>
#include <sys/stat.h>
#include <unistd.h>

int main(void)
{
    uint16_t port;
    double p;
    long v;
    char buf[64];
    uint8_t bytes[4] = { 0x00, 0x9f, 0x2c, 0xff }, back[4];

    CHECK_EQ(parse_port("8080", &port), 0); CHECK_EQ(port, 8080);
    CHECK_EQ(parse_port("0", &port), -1);
    CHECK_EQ(parse_port("65536", &port), -1);
    CHECK_EQ(parse_port("80x", &port), -1);
    CHECK_EQ(parse_port("", &port), -1);
    CHECK_EQ(parse_prob("0", &p), 0);
    CHECK_EQ(parse_prob("1e-3", &p), 0); CHECK(p == 1e-3);
    CHECK_EQ(parse_prob("1.5", &p), -1);
    CHECK_EQ(parse_prob("-0.1", &p), -1);
    CHECK_EQ(parse_prob("nan", &p), -1);
    CHECK_EQ(parse_long("16", 1, 16, &v), 0);
    CHECK_EQ(parse_long("17", 1, 16, &v), -1);

    CHECK(topic_is_valid("gatto"));
    CHECK(!topic_is_valid(""));
    CHECK(!topic_is_valid(".."));
    CHECK(!topic_is_valid("a/b"));
    CHECK(!topic_is_valid("0123456789012345678901234567890123456789012345678901234567890123"));
    CHECK(name_is_safe("OIP-x7Qm.jpeg"));
    CHECK(name_is_safe("with space.jpg"));
    CHECK(!name_is_safe(".hidden"));
    CHECK(!name_is_safe(".."));
    CHECK(!name_is_safe("a/b"));
    CHECK(!name_is_safe("bad\nname"));

    CHECK_EQ(path_join(buf, sizeof(buf), "out", "gatto"), 0); CHECK_STR(buf, "out/gatto");
    CHECK_EQ(path_join3(buf, sizeof(buf), "d", "x.jpg", ".part"), 0); CHECK_STR(buf, "d/x.jpg.part");
    CHECK_EQ(path_join(buf, 4, "out", "gatto"), -1);

    CHECK_EQ(backoff_ms(1), 50);
    CHECK_EQ(backoff_ms(2), 100);
    CHECK_EQ(backoff_ms(5), 800);
    CHECK_EQ(backoff_ms(6), 1000);
    CHECK_EQ(backoff_ms(50), 1000);

    hex_encode(bytes, 4, buf); CHECK_STR(buf, "009f2cff");
    CHECK_EQ(hex_decode("009f2cff", back, 4), 0); CHECK(memcmp(bytes, back, 4) == 0);
    CHECK_EQ(hex_decode("009f2c", back, 4), -1);
    CHECK_EQ(hex_decode("zz9f2cff", back, 4), -1);

    {
        char tmpl[] = "/tmp/fds_util_XXXXXX", deep[128];
        struct stat st;
        CHECK(mkdtemp(tmpl) != NULL);
        snprintf(deep, sizeof(deep), "%s/a/b/c", tmpl);
        CHECK_EQ(mkdir_p(deep, 0755), 0);
        CHECK_EQ(mkdir_p(deep, 0755), 0);            /* already exists: fine */
        CHECK(stat(deep, &st) == 0 && S_ISDIR(st.st_mode));
        snprintf(deep, sizeof(deep), "rm -rf %s", tmpl);
        CHECK_EQ(system(deep), 0);
    }

    {
        int64_t a = now_ns();
        sleep_ms(5);
        CHECK(now_ns() - a >= 5000000);
    }
    UNIT_DONE();
}
