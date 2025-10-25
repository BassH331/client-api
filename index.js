import 'dotenv/config';
import fetch from "node-fetch";
import express from "express";
import { createClient } from "@supabase/supabase-js";

// Connect to Supabase using environment variables (prefer service role if provided)
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY ?? process.env.SUPABASE_ANON_KEY;
const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);

// Weather API URL
const weatherUrl = "https://soil-api-vgqa.vercel.app/current";

// Target row id (edit this or set PUSH_TARGET_ID in .env)
const PUSH_TARGET_ID = process.env.PUSH_TARGET_ID ?? "e6f74576-8913-4def-9f54-fb08f926b7b2";

// Map incoming payload to primitives (strings / numbers)
function mapToPrimitives(weatherData) {
  const observedAt = weatherData.datetime_utc ? new Date(weatherData.datetime_utc) : new Date();

  const locationName = (weatherData.location && weatherData.location.name)
    ? String(weatherData.location.name)
    : "Unknown";

  const cloudiness = (weatherData.clouds && typeof weatherData.clouds.cloudiness_percent !== "undefined")
    ? Number(weatherData.clouds.cloudiness_percent)
    : null;

  const temperature = (weatherData.main && typeof weatherData.main.temperature_c !== "undefined")
    ? Number(weatherData.main.temperature_c)
    : null;

  const humidity = (weatherData.main && typeof weatherData.main.humidity_percent !== "undefined")
    ? Number(weatherData.main.humidity_percent)
    : null;

  const pressure = (weatherData.main && typeof weatherData.main.pressure_hpa !== "undefined")
    ? Number(weatherData.main.pressure_hpa)
    : null;

  const rain3h = weatherData.precipitation && weatherData.precipitation.rain_3h_mm !== undefined
    ? (weatherData.precipitation.rain_3h_mm === "N/A" ? null : Number(weatherData.precipitation.rain_3h_mm))
    : null;

  const snow3h = weatherData.precipitation && weatherData.precipitation.snow_3h_mm !== undefined
    ? (weatherData.precipitation.snow_3h_mm === "N/A" ? null : Number(weatherData.precipitation.snow_3h_mm))
    : null;

  const weatherDesc = weatherData.weather && weatherData.weather.description
    ? String(weatherData.weather.description)
    : null;

  const weatherIcon = weatherData.weather && weatherData.weather.icon
    ? String(weatherData.weather.icon)
    : null;

  const windSpeed = (weatherData.wind && typeof weatherData.wind.speed_ms !== "undefined")
    ? Number(weatherData.wind.speed_ms)
    : null;

  const windGust = (weatherData.wind && typeof weatherData.wind.gust_ms !== "undefined")
    ? Number(weatherData.wind.gust_ms)
    : null;

  const windDeg = (weatherData.wind && typeof weatherData.wind.direction_degrees !== "undefined")
    ? Number(weatherData.wind.direction_degrees)
    : null;

  // raw_payload will be a simple object with only primitives (no nested objects)
  const rawPayload = {
    icon: weatherIcon ?? null,
    humidity: humidity ?? null,
    pressure: pressure ?? null,
    wind_gust: windGust ?? null,
    wind_deg: windDeg ?? null,
    rain_3h: rain3h,
    snow_3h: snow3h
  };

  return {
    observed_at: observedAt.toISOString(),
    location: locationName,
    clouds: cloudiness,
    main: temperature,
    precipitation: rain3h ?? snow3h ?? null,
    weather: weatherDesc,
    wind: windSpeed,
    raw_payload: rawPayload
  };
}

// Insert / update the single target row using upsert (onConflict id)
async function pushObservation() {
  try {
    const res = await fetch(weatherUrl);
    if (!res.ok) throw new Error(`fetch status ${res.status}`);
    const weatherData = await res.json();

    const payload = mapToPrimitives(weatherData);

    // prepare record with explicit id so upsert updates the same row
    const record = {
      id: PUSH_TARGET_ID,
      observed_at: payload.observed_at,
      location: payload.location,       // stored as JSON scalar (string) into jsonb column
      location_point: null,
      clouds: payload.clouds,
      main: payload.main,
      precipitation: payload.precipitation,
      weather: payload.weather,
      wind: payload.wind,
      raw_payload: payload.raw_payload
    };

    // upsert with onConflict 'id' ensures the single row is updated (or created once)
    const { data, error } = await supabase
      .from("weather_observations")
      .upsert([record], { onConflict: 'id' })
      .select();

    if (error) {
      console.error("❌ Supabase upsert error:", error);
      return { ok: false, error };
    }

    return { ok: true, data };
  } catch (err) {
    console.error("❌ Error pushing observation:", err);
    return { ok: false, error: err };
  }
}

// Simple Express web client API to trigger push and run continuously
const app = express();
app.use(express.json());

// Trigger immediate push (support POST and GET for convenience)
app.post("/push-now", async (req, res) => {
  const result = await pushObservation();
  if (result.ok) return res.status(200).json({ status: "pushed", data: result.data });
  return res.status(500).json({ status: "error", error: result.error });
});

app.get("/push-now", async (req, res) => {
  const result = await pushObservation();
  if (result.ok) return res.status(200).send("pushed");
  return res.status(500).send("error");
});

// Health
app.get("/health", (req, res) => res.status(200).send("ok"));

// Start automatic pusher on launch (independent operation)
const INTERVAL_SECONDS = Number(process.env.PUSH_INTERVAL_SECONDS ?? 30);
let intervalHandle = null;

function startPusher() {
  if (intervalHandle) return;
  intervalHandle = setInterval(() => {
    pushObservation().then(r => {
      if (r.ok) console.log("✅ pushed observation", new Date().toISOString());
    });
  }, INTERVAL_SECONDS * 1000);
  // first immediate push
  pushObservation().then(r => { if (r.ok) console.log("✅ initial push done"); });
}

// allow manual control but auto-start regardless
app.post("/start", (req, res) => {
  startPusher();
  return res.json({ status: "started", interval_seconds: INTERVAL_SECONDS });
});
app.get("/start", (req, res) => {
  startPusher();
  return res.send(`started; interval_seconds=${INTERVAL_SECONDS}`);
});

app.post("/stop", (req, res) => {
  if (!intervalHandle) return res.status(400).json({ status: "not_running" });
  clearInterval(intervalHandle);
  intervalHandle = null;
  return res.json({ status: "stopped" });
});
app.get("/stop", (req, res) => {
  if (!intervalHandle) return res.status(400).send("not_running");
  clearInterval(intervalHandle);
  intervalHandle = null;
  return res.send("stopped");
});

// Start server and auto-start pusher
const PORT = Number(process.env.PORT ?? 3000);
app.listen(PORT, () => {
  console.log(`🚀 Weather push client running on port ${PORT}`);
  console.log(`Auto-starting pusher (target id=${PUSH_TARGET_ID}) every ${INTERVAL_SECONDS}s`);
  startPusher();
});
