from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

def initialize_spark(app_name="Task3_Trend_Analysis"):
    """
    Initialize and return a SparkSession.
    """
    spark = SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()
    return spark

def load_data(spark, file_path):
    """
    Load the movie ratings data from a CSV file into a Spark DataFrame.
    """
    schema = """
        UserID INT, MovieID INT, MovieTitle STRING, Genre STRING, Rating FLOAT, ReviewCount INT, 
        WatchedYear INT, UserLocation STRING, AgeGroup STRING, StreamingPlatform STRING, 
        WatchTime INT, IsBingeWatched BOOLEAN, SubscriptionStatus STRING
    """
    df = spark.read.csv(file_path, header=True, schema=schema)
    return df

def analyze_movie_watching_trends(df):
    """
    Analyze trends in movie watching over the years.
    """
    # Group by WatchedYear and count the number of movies watched
    trend_df = df.groupBy("WatchedYear").agg(count("MovieID").alias("MoviesWatched"))
    
    # Order the results by WatchedYear to identify trends over time
    trend_df = trend_df.orderBy("WatchedYear")
    
    return trend_df

def write_output(result_df, output_path):
    """
    Write the result DataFrame to a CSV file.
    """
    result_df.coalesce(1).write.csv(output_path, header=True, mode='overwrite')

def main():
    """
    Main function to execute Task 3.
    """
    # Initialize Spark session
    spark = initialize_spark()

    # Define file paths for input and output
    input_file = "/workspaces/handson-7-spark-structured-api-movie-ratings-analysis-tarunlagadapati25/input/movie_ratings_data.csv"
    output_file = "/workspaces/handson-7-spark-structured-api-movie-ratings-analysis-tarunlagadapati25/Outputs/movie_watching_trends.csv"

    # Load data into DataFrame
    df = load_data(spark, input_file)
    
    # Analyze movie watching trends
    result_df = analyze_movie_watching_trends(df)  # Call the implemented function
    
    # Write the result to an output CSV file
    write_output(result_df, output_file)

    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    main()
