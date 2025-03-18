from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

def initialize_spark(app_name="Task2_Churn_Risk_Users"):
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
    
    # Read the data from the CSV file
    df = spark.read.csv(file_path, header=True, schema=schema)
    return df

def identify_churn_risk_users(spark, df):
    """
    Identify users with canceled subscriptions and low watch time (<100 minutes).
    """
    # Filter users who have canceled subscriptions and watch time less than 100 minutes
    churn_risk_df = df.filter((col("SubscriptionStatus") == "Canceled") & (col("WatchTime") < 100))
    
    # Count the number of churn risk users
    churn_risk_count = churn_risk_df.count()

    # Count total number of users
    total_users_count = df.select("UserID").distinct().count()

    # Return the result as a DataFrame for consistency with other functions
    result_df = spark.createDataFrame([(total_users_count, churn_risk_count)], ["TotalUsers", "ChurnRiskUsers"])
    
    return result_df

def write_output(result_df, output_path):
    """
    Write the result DataFrame to a CSV file.
    """
    result_df.coalesce(1).write.csv(output_path, header=True, mode='overwrite')

def main():
    """
    Main function to execute Task 2.
    """
    # Initialize Spark session
    spark = initialize_spark()

    # Define file paths for input and output
    input_file = "/workspaces/handson-7-spark-structured-api-movie-ratings-analysis-tarunlagadapati25/input/movie_ratings_data.csv"
    output_file = "/workspaces/handson-7-spark-structured-api-movie-ratings-analysis-tarunlagadapati25/Outputs/churn_risk_users.csv"

    # Load data into DataFrame
    df = load_data(spark, input_file)
    
    # Identify churn risk users and total users
    result_df = identify_churn_risk_users(spark, df)  # Pass spark session to function
    
    # Write the result to an output CSV file
    write_output(result_df, output_file)

    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    main()
