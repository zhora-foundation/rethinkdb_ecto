defmodule TestRepo do
  use Ecto.Repo, otp_app: :rethinkdb_ecto_test,
    adapter: RethinkDB.Ecto
end

defmodule AnotherTestModel do
  use Ecto.Schema

  import Ecto.Changeset

  @primary_key {:id, :binary_id, autogenerate: false}
  @foreign_key_type :binary_id

  schema "anothers" do
    field :name, :string
  end

  @required_fields ~w(name)
  @optional_fields ~w()

  def changeset(model, params \\ :empty) do
    model
    |> cast(params, @required_fields, @optional_fields)
  end
end

defmodule TestModel do
  use Ecto.Schema

  import Ecto.Changeset

  @primary_key {:id, :binary_id, autogenerate: true}
  @foreign_key_type :binary_id

  schema "posts" do
    field :title, :string
    field :content, :string
    field :user, :string

    field :date, Ecto.DateTime, default: Ecto.DateTime.utc

    has_many :anothers, AnotherTestModel

    timestamps
  end

  @required_fields ~w(title)
  @optional_fields ~w(content user date)

  def changeset(model, params \\ :empty) do
    model
    |> cast(params, @required_fields, @optional_fields)
  end
end

Application.put_env(:rethinkdb_ecto_test, TestRepo, [database: "test"])

ExUnit.start()
