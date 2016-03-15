defmodule RethinkDB.Ecto.Transaction do
  defmacro __using__(_) do
    quote location: :keep do
      @behaviour Ecto.Adapter.Transaction

      def transaction(_module, _opts, _fun) do
        {:ok, []}
      end

      def in_transaction?(_module) do
        false
      end

      def rollback(_module, _opts) do
        :no_return
      end
    end
  end
end
