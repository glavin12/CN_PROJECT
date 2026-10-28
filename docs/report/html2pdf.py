#!/usr/bin/env python3
"""
html2pdf.py - render the final report HTML to an A4 PDF with LibreOffice.

IT305 Socket Programming Project, Group 006 + 043.

    python3 docs/report/html2pdf.py <report.html> <out.pdf>

LibreOffice's HTML import ignores CSS page size and image widths, so the report
came out as US Letter with graphs wider than the page. This script starts a
headless soffice, loads the HTML with the Writer importer, sets every page
style to A4, scales every image to the text width (keeping its aspect ratio)
and exports the PDF.
"""
import os
import subprocess
import sys
import tempfile
import time

import uno
from com.sun.star.beans import PropertyValue
from com.sun.star.awt import Size

A4_W, A4_H = 21000, 29700          # 1/100 mm
MARGIN = 1600                      # 16 mm left/right/top/bottom
MAX_IMG_W = A4_W - 2 * MARGIN - 400


def prop(name, value):
    p = PropertyValue()
    p.Name, p.Value = name, value
    return p


def main():
    if len(sys.argv) != 3:
        sys.exit("usage: html2pdf.py <report.html> <out.pdf>")
    src = os.path.abspath(sys.argv[1])
    dst = os.path.abspath(sys.argv[2])
    pipe = "fdsreport%d" % os.getpid()
    profile = tempfile.mkdtemp(prefix="lo-profile-")
    office = subprocess.Popen(
        ["soffice", "--headless", "--invisible", "--nologo", "--norestore",
         "-env:UserInstallation=file://" + profile,
         "--accept=pipe,name=%s;urp;" % pipe],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        local = uno.getComponentContext()
        resolver = local.ServiceManager.createInstanceWithContext(
            "com.sun.star.bridge.UnoUrlResolver", local)
        ctx = None
        for _ in range(100):                     # wait up to ~20 s for soffice
            try:
                ctx = resolver.resolve(
                    "uno:pipe,name=%s;urp;StarOffice.ComponentContext" % pipe)
                break
            except Exception:
                time.sleep(0.2)
        if ctx is None:
            sys.exit("html2pdf: could not connect to soffice")
        desktop = ctx.ServiceManager.createInstanceWithContext(
            "com.sun.star.frame.Desktop", ctx)
        doc = desktop.loadComponentFromURL(
            uno.systemPathToFileUrl(src), "_blank", 0,
            (prop("Hidden", True), prop("FilterName", "HTML (StarWriter)")))

        styles = doc.StyleFamilies.getByName("PageStyles")
        for name in styles.ElementNames:
            st = styles.getByName(name)
            st.Width, st.Height = A4_W, A4_H
            st.LeftMargin = st.RightMargin = MARGIN
            st.TopMargin = st.BottomMargin = MARGIN

        graphics = doc.GraphicObjects
        for i in range(graphics.Count):
            g = graphics.getByIndex(i)
            px = g.Graphic.SizePixel if g.Graphic is not None else None
            if px is None or px.Width == 0:
                continue
            w = MAX_IMG_W
            h = int(w * px.Height / px.Width)
            g.Size = Size(w, h)

        doc.storeToURL(uno.systemPathToFileUrl(dst),
                       (prop("FilterName", "writer_pdf_Export"),))
        doc.close(True)
        try:
            desktop.terminate()
        except Exception:
            pass                                 # soffice exits; the bridge drops
    finally:
        try:
            office.wait(timeout=20)
        except subprocess.TimeoutExpired:
            office.kill()
            office.wait()
        subprocess.run(["rm", "-rf", "--", profile], check=False)


if __name__ == "__main__":
    main()
