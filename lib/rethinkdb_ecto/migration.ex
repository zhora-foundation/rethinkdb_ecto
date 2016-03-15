defmodule RethinkDB.Ecto.Migration do
  defmacro __using__(_) do
    quote location: :keep do
      @behaviour Ecto.Adapter.Migration

      alias RethinkDB.Query, as: ReQL
      alias RethinkDB.Connection, as: ReC

      def supports_ddl_transaction?, do: false

      def execute_ddl(repo, {:create_if_not_exists, %Ecto.Migration.Table{name: name}, _fields}, _opts) do
        ReQL.db(repo.config[:database]) |> ReQL.table_create(name) |> repo.run
        :ok
      end

      def execute_ddl(repo, {:create, %Ecto.Migration.Table{name: name}, _fields}, _opts) do
        ReQL.db(repo.config[:database]) |> ReQL.table_create(name) |> repo.run
        :ok
      end

      def execute_ddl(repo, {:drop, %Ecto.Migration.Table{name: name}}, _opts) do
        ReQL.db(repo.config[:database]) |> ReQL.table_drop(name) |> repo.run
        :ok
      end

      def execute_ddl(repo, {:create, %Ecto.Migration.Index{columns: [column], table: table}}, _opts) do
        ReQL.db(repo.config[:database]) |> ReQL.table(table) |> ReQL.index_create(column) |> repo.run
        :ok
      end

      def execute_ddl(repo, {:drop_if_exists, %Ecto.Migration.Index{columns: [column], table: table}}, _opts) do
        ReQL.db(repo.config[:database]) |> ReQL.table(table) |> ReQL.index_drop(column) |> repo.run
        :ok
      end

      def execute_ddl(repo, {:drop, %Ecto.Migration.Index{columns: [column], table: table}}, _opts) do
        ReQL.db(repo.config[:database]) |> ReQL.table(table) |> ReQL.index_drop(column) |> repo.run
        :ok
      end
    end
  end
end
