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


class Item(BaseModel):
	sku: str
	qty: int
	price: float = 0.0


# ── plain model ──────────────────────────────────────────────────────────


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


# ── list[Model] ─────────────────────────────────────────────────────────


def _func_list_items(items: list[Item]) -> str:
	return ",".join(i.sku for i in items)


# ── dict[K, Model] ──────────────────────────────────────────────────────


def _func_dict_items(items_by_sku: dict[str, Item]) -> str:
	return ",".join(sorted(items_by_sku.keys()))


# ── Model | None ─────────────────────────────────────────────────────────


def _func_optional_addr(maybe: Address | None) -> str:
	return maybe.city if maybe else "none"


# ── list[Model] positional ────────────────────────────────────────────────


def _func_list_positional(items: list[Item]) -> str:
	return ",".join(i.sku for i in items)


# ── dict[K, Model] with nested model values ──────────────────────────────


def _func_dict_users(by_name: dict[str, User]) -> str:
	return ",".join(sorted(by_name.keys()))


# ── multiple generics ────────────────────────────────────────────────────


def _func_multi_generics(items: list[Item], by_sku: dict[str, Item]) -> str:
	return "ok"


# ── tests ─────────────────────────────────────────────────────────────────


class TestPlainModel:
	def test_coerces_dict_kwarg_to_model(self):
		payload = Payload(
			kwargs={
				"notification": {"name": "Alice", "address": {"city": "NYC", "zip_code": "10001"}}
			}
		)
		result = _coerce_payload(_func_kwarg, payload)
		assert isinstance(result.kwargs["notification"], User)
		assert result.kwargs["notification"].name == "Alice"
		assert result.kwargs["notification"].address.city == "NYC"

	def test_coerces_dict_positional_arg_to_model(self):
		payload = Payload(args=({"name": "Bob", "address": {"city": "LA", "zip_code": "90001"}},))
		result = _coerce_payload(_func_positional, payload)
		assert isinstance(result.args[0], User)
		assert result.args[0].name == "Bob"

	def test_coerces_in_mixed_args(self):
		payload = Payload(
			args=(42,),
			kwargs={
				"notification": {"name": "Eve", "address": {"city": "SF", "zip_code": "94102"}}
			},
		)
		result = _coerce_payload(_func_mixed, payload)
		assert result.args[0] == 42
		assert isinstance(result.kwargs["notification"], User)
		assert result.kwargs["notification"].name == "Eve"

	def test_skips_non_model_annotations(self):
		payload = Payload(kwargs={"notification": {"name": "Alice", "city": "NYC"}})
		result = _coerce_payload(_func_no_annotation, payload)
		assert isinstance(result.kwargs["notification"], dict)

	def test_skips_already_model_values(self):
		payload = Payload(
			kwargs={
				"notification": User(name="Alice", address=Address(city="NYC", zip_code="10001"))
			}
		)
		result = _coerce_payload(_func_kwarg, payload)
		assert isinstance(result.kwargs["notification"], User)
		assert result.kwargs["notification"].name == "Alice"

	def test_no_model_annotations_passes_through(self):
		payload = Payload(args=(1,), kwargs={"name": "test"})
		result = _coerce_payload(_func_mixed_types, payload)
		assert result.args == (1,)
		assert result.kwargs == {"name": "test"}

	def test_handles_inner_nested_models(self):
		payload = Payload(
			kwargs={
				"notification": {
					"name": "Inner",
					"address": {"city": "Berlin", "zip_code": "10115"},
				}
			}
		)
		result = _coerce_payload(_func_kwarg, payload)
		notif = result.kwargs["notification"]
		assert isinstance(notif, User)
		assert isinstance(notif.address, Address)
		assert notif.address.zip_code == "10115"


class TestListModel:
	def test_coerces_list_of_dicts_to_list_of_models(self):
		payload = Payload(
			kwargs={
				"items": [
					{"sku": "A1", "qty": 2, "price": 9.99},
					{"sku": "B2", "qty": 1},
				]
			}
		)
		result = _coerce_payload(_func_list_items, payload)
		items = result.kwargs["items"]
		assert isinstance(items, list)
		assert len(items) == 2
		assert isinstance(items[0], Item)
		assert items[0].sku == "A1"
		assert items[0].price == 9.99
		assert isinstance(items[1], Item)
		assert items[1].qty == 1
		assert items[1].price == 0.0  # default

	def test_positional_list_of_dicts(self):
		payload = Payload(
			args=([{"sku": "X1", "qty": 5}],),
		)
		result = _coerce_payload(_func_list_positional, payload)
		items = result.args[0]
		assert isinstance(items, list)
		assert isinstance(items[0], Item)
		assert items[0].sku == "X1"

	def test_already_model_list_passes_through(self):
		payload = Payload(
			kwargs={
				"items": [
					Item(sku="A1", qty=2, price=9.99),
					Item(sku="B2", qty=1),
				]
			}
		)
		result = _coerce_payload(_func_list_items, payload)
		items = result.kwargs["items"]
		assert all(isinstance(i, Item) for i in items)
		assert items[0].sku == "A1"

	def test_empty_list(self):
		payload = Payload(kwargs={"items": []})
		result = _coerce_payload(_func_list_items, payload)
		assert result.kwargs["items"] == []

	def test_mixed_model_and_dict_in_list_coerces_all(self):
		payload = Payload(
			kwargs={
				"items": [
					Item(sku="M1", qty=3),  # already a model
					{"sku": "D1", "qty": 4},  # dict
				]
			}
		)
		result = _coerce_payload(_func_list_items, payload)
		items = result.kwargs["items"]
		assert isinstance(items[0], Item)
		assert items[0].sku == "M1"
		assert isinstance(items[1], Item)
		assert items[1].sku == "D1"


class TestDictModel:
	def test_coerces_dict_values_to_models(self):
		payload = Payload(
			kwargs={
				"items_by_sku": {
					"A1": {"sku": "A1", "qty": 2, "price": 9.99},
					"B2": {"sku": "B2", "qty": 1},
				}
			}
		)
		result = _coerce_payload(_func_dict_items, payload)
		m = result.kwargs["items_by_sku"]
		assert isinstance(m, dict)
		assert isinstance(m["A1"], Item)
		assert m["A1"].price == 9.99
		assert isinstance(m["B2"], Item)
		assert m["B2"].qty == 1

	def test_dict_with_nested_models(self):
		payload = Payload(
			kwargs={
				"by_name": {
					"Alice": {
						"name": "Alice",
						"address": {"city": "NYC", "zip_code": "10001"},
					}
				}
			}
		)
		result = _coerce_payload(_func_dict_users, payload)
		users = result.kwargs["by_name"]
		assert isinstance(users["Alice"], User)
		assert isinstance(users["Alice"].address, Address)
		assert users["Alice"].address.zip_code == "10001"

	def test_already_model_dict_values(self):
		payload = Payload(
			kwargs={
				"items_by_sku": {
					"A1": Item(sku="A1", qty=2),
				}
			}
		)
		result = _coerce_payload(_func_dict_items, payload)
		assert isinstance(result.kwargs["items_by_sku"]["A1"], Item)

	def test_empty_dict(self):
		payload = Payload(kwargs={"items_by_sku": {}})
		result = _coerce_payload(_func_dict_items, payload)
		assert result.kwargs["items_by_sku"] == {}


class TestOptionalModel:
	def test_coerces_dict_to_model_in_union(self):
		payload = Payload(kwargs={"maybe": {"city": "NYC", "zip_code": "10001"}})
		result = _coerce_payload(_func_optional_addr, payload)
		assert isinstance(result.kwargs["maybe"], Address)
		assert result.kwargs["maybe"].city == "NYC"

	def test_preserves_none_in_union(self):
		payload = Payload(kwargs={"maybe": None})
		result = _coerce_payload(_func_optional_addr, payload)
		assert result.kwargs["maybe"] is None

	def test_preserves_already_model_in_union(self):
		payload = Payload(kwargs={"maybe": Address(city="LA", zip_code="90001")})
		result = _coerce_payload(_func_optional_addr, payload)
		assert isinstance(result.kwargs["maybe"], Address)
		assert result.kwargs["maybe"].city == "LA"


class TestMultipleGenerics:
	def test_coerces_list_and_dict_together(self):
		payload = Payload(
			kwargs={
				"items": [{"sku": "A1", "qty": 1}],
				"by_sku": {"B2": {"sku": "B2", "qty": 2}},
			}
		)
		result = _coerce_payload(_func_multi_generics, payload)
		assert isinstance(result.kwargs["items"][0], Item)
		assert isinstance(result.kwargs["by_sku"]["B2"], Item)
