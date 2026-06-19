#!/usr/bin/env python3
"""
make_screenshot.py — Pillow fallback screenshot generator.
Reads text from a file and renders it as a PNG image.
"""
import sys, os, textwrap
from PIL import Image, ImageDraw, ImageFont

def text_to_png(text_path: str, output_path: str, max_width: int = 800):
    """Render text file content as a PNG image."""
    with open(text_path) as f:
        text = f.read()

    # Try to find a monospace font
    font = None
    for font_path in [
        "/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf",
        "/usr/share/fonts/truetype/liberation/LiberationMono-Regular.ttf",
        "/System/Library/Fonts/Menlo.ttc",
        "C:\\Windows\\Fonts\\consola.ttf",
    ]:
        if os.path.exists(font_path):
            try:
                font = ImageFont.truetype(font_path, 14)
                break
            except Exception:
                continue

    if font is None:
        font = ImageFont.load_default()

    # Calculate line height
    try:
        line_height = font.getbbox("A")[3] + 4  # descent + 4px padding
    except Exception:
        line_height = 18

    # Wrap text to fit width
    char_width = 8
    max_chars = max_width // char_width
    lines = []
    for line in text.split("\n"):
        if line:
            wrapped = textwrap.wrap(line, width=max_chars) or [""]
            lines.extend(wrapped)
        else:
            lines.append("")

    # Calculate image size
    img_height = max(len(lines) * line_height + 40, 100)
    img = Image.new("RGB", (max_width, img_height), (40, 42, 54))  # dracula bg
    draw = ImageDraw.Draw(img)

    y = 10
    for line in lines:
        draw.text((10, y), line, font=font, fill=(248, 248, 242))  # dracula fg
        y += line_height

    img.save(output_path, "PNG")
    print(f"  ✅ Saved {output_path} ({img.size[0]}x{img.size[1]})")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: make_screenshot.py <input.txt> <output.png>")
        sys.exit(1)
    text_to_png(sys.argv[1], sys.argv[2])
