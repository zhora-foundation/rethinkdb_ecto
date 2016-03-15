defmodule MigrationTest do
  use ExUnit.Case

  alias RethinkDB.Record
  alias RethinkDB.Query, as: ReQL

  @table_name "test_table"

  defmodule CreateTableMigrationTest do
    use Ecto.Migration

    def change do
      create table(:test_table)
      create index(:test_table, [:name])
    end
  end

  setup_all do
    {:ok, _} = TestRepo.start_link

    on_exit fn ->
      TestRepo.start_link
      ReQL.table_drop("schema_migrations") |> TestRepo.run
    end

    :ok
  end

  test "create and drop table" do
    Ecto.Migrator.up(TestRepo, 1, CreateTableMigrationTest, [])
    %Record{data: data} = ReQL.table_list |> TestRepo.run

    assert Enum.find(data, &(&1 == @table_name))

    Ecto.Migrator.down(TestRepo, 1, CreateTableMigrationTest, [])
    %Record{data: data} = ReQL.table_list |> TestRepo.run

    assert Enum.find(data, &(&1 == @table_name)) == nil
  end
end
