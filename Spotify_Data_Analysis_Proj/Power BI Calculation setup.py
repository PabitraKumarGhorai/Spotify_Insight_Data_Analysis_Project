# Databricks notebook source
# MAGIC %sql
# MAGIC select * from default.DIM_SPOTIFY_TRACK_TABLE

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from default.DIM_SPOTIFY_ARTIST_TABLE

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from default.FACT_SPOTIFY_FOUNDATION_TABLE

# COMMAND ----------

# DBTITLE 1,Top 20 Artist who has sung maximum songs
# MAGIC %sql
# MAGIC select Artist_Name, sum(Total_Track) from default.dim_spotify_artist_table 
# MAGIC group by Artist_Name
# MAGIC order by sum(Total_Track) desc
# MAGIC limit 20

# COMMAND ----------

# DBTITLE 1,Top 20 High Popular Tracks
# MAGIC %sql
# MAGIC select * from default.dim_spotify_track_table where Track_Popularity_Level = "High Popular Track"
# MAGIC order by Popularity desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from default.fact_spotify_foundation_table  where Artist_Name = "Grateful Dead"

# COMMAND ----------

# DBTITLE 1,Year wise Total no of track and total number of Artist
# MAGIC %sql
# MAGIC select sum(Total_Track), count(Artist_ID), Year from default.fact_spotify_foundation_table
# MAGIC group by Year
# MAGIC order by Year asc

# COMMAND ----------

# DBTITLE 1,Top 20 Genre by Popularity
# MAGIC %sql
# MAGIC select Genre, sum(Popularity) from default.fact_spotify_foundation_table
# MAGIC group by Genre 
# MAGIC order by sum(Popularity) desc
# MAGIC limit 20

# COMMAND ----------

