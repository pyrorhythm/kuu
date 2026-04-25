from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field


class Result(BaseModel):
	model_config = ConfigDict(frozen=True)

	status: Literal["ok", "error"]
	value: Annotated[bytes | None, Field(default=None)]
	error: Annotated[str | None, Field(default=None)]
	type: Annotated[str | None, Field(default=None)]
