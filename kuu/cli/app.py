from __future__ import annotations

import logging

from typer import Typer

from kuu.cli._info import app as info_app
from kuu.cli._start import app as worker_app

log = logging.getLogger("kuu.cli")

app = Typer(name="kuu")
app.add_typer(worker_app)
app.add_typer(info_app)
