defmodule ConfigTest do
  use ExUnit.Case

  import Mock

  test_with_mock "it should use config", RethinkDB.Ecto.Connection, [:passthrough], [] do
    Application.put_env(:rethinkdb_ecto_test, TestRepo,
      [hostname: "127.0.0.8", port: 1, db: "t", auth_key: "hi"])

    {:ok, pid} = TestRepo.start_link

    assert called(RethinkDB.Ecto.Connection.start_link(
      [otp_app: :rethinkdb_ecto_test, repo: TestRepo,
       hostname: "127.0.0.8", port: 1, db: "t", auth_key: "hi"]))

    TestRepo.stop(pid)
  end
end
