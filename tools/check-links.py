#!/usr/bin/env python3
"""Verify the document's evidence layer and cross-references.

Three things rot silently and all three have already happened once: a probe link
pointing at a test that was renamed, an anchor left behind by a retitled heading,
and a relative path to a file that moved. Nothing else here checks them.

Usage: python3 tools/check-links.py [file.md ...]     (default: every tracked .md)
"""
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

LINK = re.compile(r'\[([^\]]*)\]\(([^)\s]+)(?:\s+"([^"]*)")?\)')
HEADING = re.compile(r'^(#{1,6})\s+(.*?)\s*$', re.M)
FENCE = re.compile(r'^```', re.M)


def slug(text):
    """GitHub's heading-anchor algorithm, close enough for our headings."""
    text = re.sub(r'\[([^\]]*)\]\([^)]*\)', r'\1', text)   # links to their text
    text = re.sub(r'[`*_~]', '', text)                      # inline formatting
    text = text.lower()
    text = re.sub(r'[^a-z0-9 \-]', '', text)
    return text.replace(' ', '-')


def strip_fences(text):
    """Blank out code so links written as examples are not parsed as links."""
    out, fenced = [], False
    for line in text.split('\n'):
        if line.lstrip().startswith('```'):
            fenced = not fenced
            out.append('')
        else:
            out.append('' if fenced else re.sub(r'`[^`]*`', '', line))
    return '\n'.join(out)


def check(path, anchors_by_file):
    text = path.read_text()
    body = strip_fences(text)
    problems = []

    for m in LINK.finditer(body):
        label, target, title = m.group(1), m.group(2), m.group(3)
        line = body[:m.start()].count('\n') + 1
        where = f'{path.name}:{line}'

        if target.startswith(('http://', 'https://', 'mailto:')):
            continue

        anchor = None
        if '#' in target:
            target, anchor = target.split('#', 1)

        if target:
            resolved = (path.parent / target).resolve()
            if not resolved.exists():
                problems.append(f'{where}: [{label}] -> missing path {target}')
                continue
        else:
            resolved = path

        if anchor is not None:
            known = anchors_by_file.get(resolved)
            if known is None and resolved.suffix == '.md':
                known = anchors_by_file[resolved] = {
                    slug(h) for _, h in HEADING.findall(strip_fences(resolved.read_text()))
                }
            if known is not None and anchor not in known:
                problems.append(f'{where}: [{label}] -> no heading #{anchor} in {resolved.name}')

        # A probe link claims a specific test exists. That is the claim worth checking.
        if label == 'probe':
            if not title:
                problems.append(f'{where}: probe link names no test method')
            elif f'void {title}(' not in resolved.read_text():
                problems.append(f'{where}: probe link -> no test {title}() in {resolved.name}')

    return problems


def main():
    targets = [Path(a) for a in sys.argv[1:]] or sorted(ROOT.glob('*.md'))
    anchors = {}
    problems = []
    for t in targets:
        problems += check(t.resolve(), anchors)

    for p in problems:
        print(p)
    total = sum(len(LINK.findall(strip_fences(t.read_text()))) for t in targets)
    print(f'\n{len(problems)} problem(s) across {total} links in {len(targets)} file(s)')
    return 1 if problems else 0


if __name__ == '__main__':
    sys.exit(main())
