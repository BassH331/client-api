import 'dotenv/config';
import fetch from "node-fetch";
import { createClient } from "@supabase/supabase-js";

// Connect to Supabase using environment variables
const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_ANON_KEY
);

// Weather API URL
const weatherUrl = "https://soil-api-vgqa.vercel.app/current";

// CRUD Operations for Weather Data
async function createWeatherObservation(farmName, latitude, longitude, data) {
  const { data: result, error } = await supabase
    .from("weather_observations")
    .insert([{
      farm_name: farmName,
      latitude: latitude,
      longitude: longitude,
      data: data,
      created_at: new Date()
    }])
    .select();

  if (error) throw error;
  return result;
}

async function getWeatherObservations(options = {}) {
  let query = supabase
    .from("weather_observations")
    .select("*");

  if (options.farmName) {
    query = query.eq('farm_name', options.farmName);
  }
  if (options.dateRange) {
    query = query.gte('created_at', options.dateRange.start)
                .lte('created_at', options.dateRange.end);
  }
  if (options.limit) {
    query = query.limit(options.limit);
  }

  const { data, error } = await query;
  if (error) throw error;
  return data;
}

async function updateWeatherObservation(id, updates) {
  const { data, error } = await supabase
    .from("weather_observations")
    .update(updates)
    .eq('id', id)
    .select();

  if (error) throw error;
  return data;
}

async function deleteWeatherObservation(id) {
  const { error } = await supabase
    .from("weather_observations")
    .delete()
    .eq('id', id);

  if (error) throw error;
  return true;
}

// Modified seed function to use new CRUD operations
async function seedWeatherData() {
  try {
    const weatherRes = await fetch(weatherUrl);
    const weatherData = await weatherRes.json();
    
    const result = await createWeatherObservation(
      "University of Mpumalanga",
      -25.43542,
      30.98083,
      weatherData
    );

    console.log("✅ Weather data stored successfully!", result);
  } catch (error) {
    console.error("❌ Error seeding weather data:", error);
  }
}

// Example usage of CRUD operations
async function example() {
  try {
    // Create initial data
    await seedWeatherData();

    // Read weather data with filters
    const recentData = await getWeatherObservations({
      farmName: "University of Mpumalanga",
      limit: 5
    });
    console.log("📊 Recent weather observations:", recentData);

    // Update if we have data
    if (recentData.length > 0) {
      const updated = await updateWeatherObservation(recentData[0].id, {
        farm_name: "UMP Updated"
      });
      console.log("✏️ Updated record:", updated);
    }

    // Delete oldest record
    if (recentData.length > 0) {
      await deleteWeatherObservation(recentData[0].id);
      console.log("🗑️ Deleted oldest record");
    }

  } catch (error) {
    console.error("❌ Error in example:", error);
  }
}

// Run the example
example();
