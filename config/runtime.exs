import Config

config :plausible, Plausible.Cache.Adapter, sessions: [partitions: System.schedulers()]
