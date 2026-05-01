from __future__ import annotations

from pydantic import BaseModel

from kuu.message import Payload
from kuu.worker import _coerce_payload


class Address(BaseModel):
	city: str
	zip_code: str


class User(BaseModel):
	name: str
	address: Address


def _func_kwarg(notification: User) -> str:
	return notification.name


def _func_positional(notification: User) -> str:
	return notification.name


def _func_mixed(x: int, notification: User) -> str:
	return f"{x}:{notification.name}"


def _func_no_annotation(notification: dict) -> dict:
	return notification


def _func_mixed_types(x: int, name: str) -> str:
	return f"{x}:{name}"


def test_coerces_dict_kwarg_to_model():
	payload = Payload(
		kwargs={"notification": {"name": "Alice", "address": {"city": "NYC", "zip_code": "10001"}}}
	)
	result = _coerce_payload(_func_kwarg, payload)
	assert isinstance(result.kwargs["notification"], User)
	assert result.kwargs["notification"].name == "Alice"
	assert result.kwargs["notification"].address.city == "NYC"


def test_coerces_dict_positional_arg_to_model():
	payload = Payload(args=({"name": "Bob", "address": {"city": "LA", "zip_code": "90001"}},))
	result = _coerce_payload(_func_positional, payload)
	assert isinstance(result.args[0], User)
	assert result.args[0].name == "Bob"


def test_coerces_in_mixed_args():
	payload = Payload(
		args=(42,),
		kwargs={"notification": {"name": "Eve", "address": {"city": "SF", "zip_code": "94102"}}},
	)
	result = _coerce_payload(_func_mixed, payload)
	assert result.args[0] == 42
	assert isinstance(result.kwargs["notification"], User)
	assert result.kwargs["notification"].name == "Eve"


def test_skips_non_model_annotations():
	payload = Payload(kwargs={"notification": {"name": "Alice", "city": "NYC"}})
	result = _coerce_payload(_func_no_annotation, payload)
	# dict stays dict — no BaseModel annotation to coerce to
	assert isinstance(result.kwargs["notification"], dict)


def test_skips_already_model_values():
	payload = Payload(
		kwargs={"notification": User(name="Alice", address=Address(city="NYC", zip_code=" "))}
	)
	result = _coerce_payload(_func_kwarg, payload)
	assert isinstance(result.kwargs["notification"], User)
	assert result.kwargs["notification"].name == "Alice"


def test_no_model_annotations_passes_through():
	payload = Payload(args=(1,), kwargs={"name": "test"})
	result = _coerce_payload(_func_mixed_types, payload)
	assert result.args == (1,)
	assert result.kwargs == {"name": "test"}


def test_handles_inner_nested_models():
	payload = Payload(
		kwargs={
			"notification": {"name": "Inner", "address": {"city": "Berlin", "zip_code": "10115"}}
		}
	)
	result = _coerce_payload(_func_kwarg, payload)
	notif = result.kwargs["notification"]
	assert isinstance(notif, User)
	assert isinstance(notif.address, Address)
	assert notif.address.zip_code == "10115"
