defmodule QueryTest do
  use ExUnit.Case

  import Ecto.Query, only: [from: 2]

  alias RethinkDB.Query, as: ReQL
  alias RethinkDB.Connection, as: ReC

  setup do
    Application.put_env(:rethinkdb_ecto_test, TestRepo, [])

    {:ok, conn} = ReC.start_link
    ReQL.table_create("posts") |> ReC.run(conn)
    ReQL.table("posts") |> ReQL.delete |> ReC.run(conn)

    {:ok, _} = TestRepo.start_link
    {:ok, model} = TestRepo.insert(%TestModel{title: "yay", date: Ecto.DateTime.utc})

    {:ok, model: model}
  end

  test "insert queries work", %{model: model} do
    assert model.title == "yay"
  end

  test "insert queries with Ecto.DateTime should work" do
    dt = Ecto.DateTime.utc
    {:ok, model} = TestRepo.insert(%TestModel{date: dt})
    assert model.date == dt
  end

  test "insert many models" do
    result = TestRepo.insert_all(TestModel,
      [%{title: "1"}, %{title: "2"}, %{title: "3"}])

    assert result == {3, []}
  end

  test "get one queries work", %{model: model} do
    from_db = TestRepo.get(TestModel, model.id)
    assert model == from_db
  end

  test "get many queries work"  do
    TestModel |> TestRepo.all |> Enum.map(&(TestRepo.delete(&1)))

    {:ok, model} = TestRepo.insert(%TestModel{title: "yay"})
    {:ok, model_2} = TestRepo.insert(%TestModel{title: "yayay"})
    from_db = TestRepo.all(TestModel)
    assert model in from_db
    assert model_2 in from_db
  end

  test "filtered queries work", %{model: _}  do
    {:ok, _} = TestRepo.insert(%TestModel{title: "yoyo"})

    from_db =
      TestRepo.all(from m in TestModel, where: m.title == "yoyo")
      |> List.first

    assert from_db.title == "yoyo"
  end

  test "update queries work", %{model: model} do
    update_changeset = TestModel.changeset(model, %{title: "yayay"})
    {:ok, updated_model} = TestRepo.update(update_changeset)
    assert updated_model.title == "yayay"
  end

  test "delete queries work", %{model: model} do
    TestRepo.delete(model)
    from_db = TestRepo.get(TestModel, model.id)
    refute from_db
  end
end
