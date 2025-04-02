defmodule Plausible.Factory do
  use ExMachina.Ecto, repo: Plausible.Repo

  def ch_session_factory do
    hostname = sequence(:domain, &"example-#{&1}.com")

    %Plausible.ClickhouseSessionV2{
      sign: 1,
      session_id: SipHash.hash!(hash_key(), Ecto.UUID.generate()),
      user_id: SipHash.hash!(hash_key(), Ecto.UUID.generate()),
      hostname: hostname,
      site_id: Enum.random(1000..10_000),
      entry_page: "/",
      pageviews: 1,
      events: 1,
      start: NaiveDateTime.utc_now(:seocnd),
      timestamp: NaiveDateTime.utc_now(:seocnd),
      is_bounce: false
    }
  end

  def pageview_factory(attrs) do
    Map.put(event_factory(attrs), :name, "pageview")
  end

  def engagement_factory(attrs) do
    Map.put(event_factory(attrs), :name, "engagement")
  end

  def event_factory(attrs) do
    if Map.get(attrs, :acquisition_channel) do
      raise "Acquisition channel cannot be written directly since it's a materialized column."
    end

    hostname = sequence(:domain, &"example-#{&1}.com")

    event = %Plausible.ClickhouseEventV2{
      hostname: hostname,
      site_id: Enum.random(1000..10_000),
      pathname: "/",
      timestamp: NaiveDateTime.utc_now(:second),
      user_id: SipHash.hash!(hash_key(), Ecto.UUID.generate()),
      session_id: SipHash.hash!(hash_key(), Ecto.UUID.generate())
    }

    event
    |> merge_attributes(attrs)
    |> evaluate_lazy_attributes()
  end

  defp hash_key do
    <<95, 201, 93, 230, 168, 247, 88, 20, 19, 150, 34, 50, 205, 173, 189, 65>>
  end
end
