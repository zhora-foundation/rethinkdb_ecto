defmodule RethinkDB.Ecto do
  @behaviour Ecto.Adapter

  use RethinkDB.Ecto.Migration
  use RethinkDB.Ecto.Storage
  # use RethinkDB.Ecto.Transaction

  alias RethinkDB.Query
  alias RethinkDB.Ecto.NormalizedQuery

  alias Supervisor.Spec

  defmodule Connection do
    use RethinkDB.Connection
  end

  defmacro __before_compile__(_env) do
    quote do
      defdelegate run(query), to: Connection
      defdelegate run(query, opts), to: Connection
    end
  end

  def application, do: RethinkDB.Ecto

  def child_spec(_repo, opts), do: Spec.worker(Connection, [opts])

  def loaders(:datetime, _type), do: [&load_date/1]
  def loaders(:date, _type), do: [&load_date/1]
  def loaders(:time, _type), do: [&load_date/1]
  def loaders(_primitive, type), do: [type]

  def dumpers(:datetime, _type), do: [&dump_date/1]
  def dumpers(:date, _type), do: [&dump_date/1]
  def dumpers(:time, _type), do: [&dump_date/1]
  def dumpers(_primitive, type), do: [type]

  def autogenerate(:id), do: nil
  def autogenerate(:binary_id), do: Ecto.UUID.generate

  def prepare(fun, query), do: {:nocache, {fun, query}}

  def execute(repo, meta, {_, {fun, query}}, params, process, opts) do
    fields =
      case meta.sources do
        {{_, nil}}   -> []
        {{_, model}} -> model.__schema__(:fields)
      end

    apply(NormalizedQuery, fun, [query, params, opts])
    |> run(repo, {fun, meta.fields}, [], process, fields)
  end

  def insert_all(repo, meta, _header, fields, returning, opts) do
    NormalizedQuery.insert_all(meta, fields, opts)
    |> run(repo, {:insert_all, fields}, returning, nil, [])
  end

  def insert(repo, meta, fields, returning, opts) do
    NormalizedQuery.insert(meta, fields, opts)
    |> run(repo, {:insert, fields}, returning, nil, [])
  end

  def update(repo, meta, fields, filters, _returning, _opts) do
    NormalizedQuery.update(meta, fields, filters)
    |> run(repo, {:update, fields}, [], nil, [])
  end

  def delete(repo, meta, filters, _opts) do
    NormalizedQuery.delete(meta, filters)
    |> run(repo, {:delete, []}, [], nil, [])
  end

  defp run(query, repo, {func, fields}, _returning, process, schema_fields) do
    case repo.run(query) do
      %{data: %{"r" => [error|_]}} ->
        {:invalid, [error: error]}
      %{data: %{"first_error" => error}} ->
        {:invalid, [error: error]}
      %{data: %{"generated_keys" => [_id|_]}} ->
        # {:ok, Keyword.put(fields, :d, id)}
        {:ok, fields}
      %{data: []} ->
        {:ok, []}
      %{data: data} when is_list(data) ->
        try do
          {records, count} = Enum.map_reduce(data, 0,
            &{process_record(&1, process, fields, schema_fields), &2 + 1})
          {count, records}
        catch
          :error, {:badmap, 1} ->
            {:ok, [data]}
        end
      %{data: data} ->
        case func do
          :update_all ->
            {Map.get(data, "replaced", 0), []}
          :delete_all ->
            {Map.get(data, "deleted", 0), []}
          :insert_all ->
            {Map.get(data, "inserted", 0), []}
          _ ->
            {:ok, fields}
        end
    end
  end

  defp process_record(record, preprocess, args, _) when is_list(record) do
    Enum.map_reduce(record, args, fn record, [expr|exprs] ->
      {preprocess.(expr, record, nil), exprs}
    end) |> elem(0)
  end

  # FIXME
  defp process_record(record, preprocess, expr, schema_fields) do
    record =
      List.foldl(schema_fields, [], fn field, acc ->
        case Map.get(record, Atom.to_string(field), :undefined) do
          :undefined -> [nil | acc]
          value -> [value | acc]
        end
      end)
      |> Enum.reverse

    Enum.map(expr, &preprocess.(&1, record, nil))
  end

  defp dump_date(%Ecto.Date{year: year, month: month, day: day}),
    do: dump_date({{year, month, day}, {0, 0, 0, 0}})
  defp dump_date(%Ecto.Time{hour: hour, min: min, sec: sec, usec: usec}),
    do: dump_date({{1970, 1, 1}, {hour, min, sec, usec}})
  defp dump_date(%Ecto.DateTime{year: year, month: month, day: day, hour: hour, min: min, sec: sec, usec: usec}),
    do: dump_date({{year, month, day}, {hour, min, sec, usec}})
  defp dump_date({_, _, _, _} = time),
    do: dump_date({{1970, 1, 1}, time})
  defp dump_date({{year, month, day}, {hour, min, sec, _}}) do
    {:ok, Query.time(year, month, day, hour, min, sec, "Z")}
  end

  defp load_date({{y, m, d}, {hh, mm, ss, _}}), do:
    {:ok, Ecto.DateTime.from_erl({{y, m, d}, {hh, mm, ss}})}
  defp load_date(%RethinkDB.Q{query: [136, [y, m, d, hh, mm, ss, _]]}), do:
    {:ok, Ecto.DateTime.from_erl({{y, m, d}, {hh, mm, ss}})}
  defp load_date(%RethinkDB.Pseudotypes.Time{epoch_time: epoch_time}) do
    date_time =
      epoch_time
      |> round
      |> +(62167219200) # :calendar.datetime_to_gregorian_seconds({{1970, 1, 1}, {0, 0, 0}})
      |> :calendar.gregorian_seconds_to_datetime
      |> Ecto.DateTime.from_erl

    {:ok, date_time}
  end

  defp load_date(_), do: :error
end
