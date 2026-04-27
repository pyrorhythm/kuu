from __future__ import annotations

import sys
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

with (ROOT / "pyproject.toml").open("rb") as fd:
	_pyproject = tomllib.load(fd)

project = "kuu"
author = "Alexey Pechenin"
release = _pyproject["project"]["version"]
version = release

extensions = [
	"myst_parser",
	"autodoc2",
	"sphinx.ext.intersphinx",
	"sphinx.ext.viewcode",
	"sphinx_copybutton",
]

source_suffix = {
	".md": "markdown",
	".rst": "restructuredtext",
}

exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]
templates_path = ["_templates"]

myst_enable_extensions = [
	"colon_fence",
	"deflist",
	"fieldlist",
	"linkify",
	"smartquotes",
	"tasklist",
]
myst_heading_anchors = 3

autodoc2_packages = [
	{
		"path": "../kuu",
		"auto_mode": True,
	},
]
autodoc2_render_plugin = "myst"
autodoc2_output_dir = "apidocs"
autodoc2_hidden_objects = ["dunder", "private", "inherited"]
autodoc2_class_inheritance = True

intersphinx_mapping = {
	"python": ("https://docs.python.org/3", None),
	"anyio": ("https://anyio.readthedocs.io/en/stable", None),
	"pydantic": ("https://docs.pydantic.dev/latest", None),
}

pygments_style = "monokai"

html_theme = "furo"
html_title = f"kuu {version}"
html_static_path = ["_static"]
html_css_files = ["custom.css"]

html_theme_options = {
	"light_logo": "kuu-logo.svg",
	"dark_logo": "kuu-logo-white.svg",
	"source_repository": "https://github.com/pyrorhythm/kuu",
	"source_branch": "master",
	"source_directory": "docs/",
	"sidebar_hide_name": False,
	"navigation_with_keys": True,
	"footer_icons": [
		{
			"name": "github",
			"url": "https://github.com/pyrorhythm/kuu",
			"html": """<svg stroke="currentColor" fill="currentColor" stroke-width="0" viewBox="0 0 16 16">
                    <path fill-rule="evenodd" d="M8 0C3.58 0 0 3.58 0 8c0 3.54 2.29 6.53 5.47 7.59.4.07.55-.17.55-.38 0-.19-.01-.82-.01-1.49-2.01.37-2.53-.49-2.69-.94-.09-.23-.48-.94-.82-1.13-.28-.15-.68-.52-.01-.53.63-.01 1.08.58 1.23.82.72 1.21 1.87.87 2.33.66.07-.52.28-.87.51-1.07-1.78-.2-3.64-.89-3.64-3.95 0-.87.31-1.59.82-2.15-.08-.2-.36-1.02.08-2.12 0 0 .67-.21 2.2.82.64-.18 1.32-.27 2-.27.68 0 1.36.09 2 .27 1.53-1.04 2.2-.82 2.2-.82.44 1.1.16 1.92.08 2.12.51.56.82 1.27.82 2.15 0 3.07-1.87 3.75-3.65 3.95.29.25.54.73.54 1.48 0 1.07-.01 1.93-.01 2.2 0 .21.15.46.55.38A8.013 8.013 0 0 0 16 8c0-4.42-3.58-8-8-8z"></path>
                </svg>""",
			"class": "",
		},
	],
	"light_css_variables": {
		"font-stack": "'Geist', 'Geist Sans', system-ui, -apple-system, sans-serif",
		"font-stack--monospace": "'Geist Mono', ui-monospace, monospace",
	},
	"dark_css_variables": {
		"font-stack": "'Geist', 'Geist Sans', system-ui, -apple-system, sans-serif",
		"font-stack--monospace": "'Geist Mono', ui-monospace, monospace",
	},
}

pygments_style = "friendly_grayscale"
pygments_dark_style = "friendly_grayscale"

copybutton_prompt_text = r">>> |\.\.\. |\$ |# "
copybutton_prompt_is_regexp = True

copyright = "2026, Alexey Pechenin <me@pyrorhythm.dev>"

html_favicon = "_static/kuu-moon.svg"
