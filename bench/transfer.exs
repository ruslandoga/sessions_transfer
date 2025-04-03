alias Plausible.Session.Transfer
alias Plausible.Session.Transfer.TinySock

:telemetry.attach(
  "bench-transfer",
  Plausible.Session.Transfer.telemetry_event(),
  fn _event, measurements, _meta, _config ->
    IO.inspect(measurements, label: "transfer measurements")
  end,
  nil
)

Benchee.run(
  %{
    "try_take_all_ets_everywhere" => fn base_path ->
      Transfer.try_take_all_ets_everywhere(base_path)
    end
  },
  before_scenario: fn _ ->
    base_path = "./sessions"
    TinySock.start_link(base_path: base_path, handler: &Transfer.giver_handler/1)
    base_path
  end,
  before_each: fn base_path ->
    task =
      Task.async(fn ->
        Plausible.Cache.Adapter.get_names(:sessions)
        |> Enum.each(fn name -> :ets.delete_all_objects(ConCache.ets(name)) end)

        sessions =
          Enum.map(1..200_000, fn i ->
            session = %Plausible.ClickhouseSessionV2{
              hostname: "example-0.com",
              site_id: 1053,
              user_id: i,
              session_id: 10_371_982_002_942_482_638,
              start: ~N[2025-04-02 19:51:32],
              duration: 0,
              is_bounce: true,
              entry_page: "/",
              exit_page: "/",
              exit_page_hostname: "example-0.com",
              pageviews: 1,
              events: 1,
              sign: 1,
              "entry_meta.key": nil,
              "entry_meta.value": nil,
              utm_medium: "medium",
              utm_source: "source",
              utm_campaign: "campaign",
              utm_content: "content",
              utm_term: "term",
              referrer: "ref",
              referrer_source: "refsource",
              click_id_param: nil,
              country_code: "EE",
              subdivision1_code: nil,
              subdivision2_code: nil,
              city_geoname_id: nil,
              screen_size: "Desktop",
              operating_system: "Mac",
              operating_system_version: "11",
              browser: "browser",
              browser_version: "55",
              timestamp: ~N[2025-04-02 19:51:32],
              transferred_from: nil,
              acquisition_channel: nil
            }

            {{session.site_id, session.user_id}, session}
          end)

        Plausible.Cache.Adapter.put_many(:sessions, sessions)
      end)

    Task.await(task)
    base_path
  end
  # profile_after: :tprof
)
