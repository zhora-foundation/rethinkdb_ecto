defmodule RethinkDB.Ecto.Storage do
  defmacro __using__(_) do
    quote location: :keep do
      @behaviour Ecto.Adapter.Storage

      alias RethinkDB.Record
      alias RethinkDB.Response
      alias RethinkDB.Query, as: ReQL
      alias RethinkDB.Connection, as: ReC

      def storage_up(opts) do
        db = Dict.fetch!(opts, :db)
        # TODO: add host and port
        {:ok, pid} = RethinkDB.Connection.start_link([])
        result = ReQL.db_create(db) |> ReC.run(pid)
        case result do
          %Response{data: %{"e" => 4100000, "r" => r}} ->
            already_exists = "Database `#{db}` already exists."
            case r do
              [^already_exists] -> {:error, :already_up}
              _ -> {:error, r}
            end
          %Record{data: %{"dbs_created" => 1}} -> :ok
        end
      end

      def storage_down(opts) do
        IO.puts(" ---------- storage_down")
        IO.inspect(opts)
        db = Dict.fetch!(opts, :db)
        # TODO: add host and port
        {:ok, pid} = RethinkDB.Connection.start_link([])
        result = ReQL.db_drop(db) |> ReC.run(pid)
        case result do
          %Response{data: %{"e" => 4100000, "r" => r}} ->
            does_not_exist = "Database `#{db}` does not exist."
            case r do
              [^does_not_exist]-> {:error, :already_down}
              _ -> {:error, r}
            end
          %Record{data: %{"dbs_dropped" => 1}} -> :ok
        end
      end
    end
  end
end
