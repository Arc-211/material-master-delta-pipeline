# Databricks notebook source
# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os
from datetime import datetime

print("=== ASSIGNMENT EVIDENCE ===")
print(f"Execution Time: {datetime.now()}")
print(f"Workspace URL: {spark.conf.get('spark.databricks.workspaceUrl', 'N/A')}")
print(f"Spark Version: {spark.version}")

# COMMAND ----------

# Use the same volume where your source file is located
SOURCE_PATH = "/Volumes/quiz_solution/material_master_pipeline/notebooks/DBC 10 Material Master.csv"

print(f"Source: {SOURCE_PATH}")

# COMMAND ----------

# Define schema
schema = StructType([
    StructField("material_id", StringType(), True),
    StructField("material_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("sub_category", StringType(), True),
    StructField("uom", StringType(), True),
    StructField("unit_cost", StringType(), True),
    StructField("supplier_name", StringType(), True),
    StructField("country", StringType(), True),
    StructField("plant", StringType(), True),
    StructField("status", StringType(), True),
    StructField("last_updated", StringType(), True),
    StructField("lead_time_days", StringType(), True),
    StructField("safety_stock", StringType(), True),
    StructField("reorder_level", StringType(), True),
    StructField("remarks", StringType(), True)
])

print("✅ Schema defined")

# COMMAND ----------

# Read data
print("Reading source data...")
raw_df = (spark.read
          .option("delimiter", "|")
          .option("header", "true")
          .schema(schema)
          .csv(SOURCE_PATH))

record_count = raw_df.count()
print(f"✅ Read {record_count} records")

print("\n=== RAW DATA SAMPLE ===")
raw_df.show(3, truncate=False)

# COMMAND ----------

# Add metadata and create Bronze DataFrame in memory
print("Processing data for Bronze layer...")

bronze_df = (raw_df
             .withColumn("ingestion_timestamp", current_timestamp())
             .withColumn("ingestion_date", current_date())
             .withColumn("source_file", lit("DBC 10 Material Master.csv"))
             .withColumn("pipeline_run_id", lit(datetime.now().strftime("%Y%m%d_%H%M%S"))))

print("✅ Bronze DataFrame created in memory")
print(f"📊 Total records: {bronze_df.count()}")

# COMMAND ----------

# Create temporary view instead of saving to Delta (to avoid file system issues)
bronze_df.createOrReplaceTempView("material_master_bronze")

print("✅ Bronze temporary table created: material_master_bronze")

# Show Bronze data sample
print("\n=== BRONZE DATA SAMPLE ===")
spark.sql("SELECT material_id, material_name, unit_cost, status, ingestion_timestamp FROM material_master_bronze LIMIT 5").show(truncate=False)

# COMMAND ----------

# Data Quality Assessment on Bronze data
print("=== DATA QUALITY ASSESSMENT ===")

# Get stats using SQL
total_records = spark.sql("SELECT COUNT(*) as count FROM material_master_bronze").collect()[0]['count']
print(f"Total Records: {total_records}")

# Check for abc values
abc_count = spark.sql("SELECT COUNT(*) as count FROM material_master_bronze WHERE unit_cost = 'abc'").collect()[0]['count']
print(f"Unit cost 'abc' values: {abc_count}")

# Status distribution
print("\n📊 Status Distribution:")
spark.sql("SELECT status, COUNT(*) as count FROM material_master_bronze GROUP BY status ORDER BY count DESC").show()

# Plant distribution
print("🏭 Plant Distribution:")
spark.sql("SELECT plant, COUNT(*) as count FROM material_master_bronze GROUP BY plant ORDER BY plant").show()

# Show some problem records
if abc_count > 0:
    print("\n❗ Sample records with 'abc' unit_cost:")
    spark.sql("SELECT material_id, material_name, unit_cost FROM material_master_bronze WHERE unit_cost = 'abc' LIMIT 3").show()

print("\n🎉 BRONZE LAYER PROCESSING COMPLETED!")
print("📋 Data available in temporary view: material_master_bronze")

# COMMAND ----------

# Summary for assignment
print("=" * 60)
print("BRONZE LAYER IMPLEMENTATION SUMMARY")
print("=" * 60)
print("✅ Successfully read pipe-delimited CSV file")
print("✅ Applied proper schema with 15 business columns")
print("✅ Added metadata columns for data lineage")
print("✅ Created Bronze temporary table for analysis")
print("✅ Performed comprehensive data quality assessment")
print("\n📋 ASSIGNMENT EVIDENCE:")
print(f"   - User: chonkar.a@northeastern.edu (implied)")
print(f"   - File: DBC 10 Material Master.csv")
print(f"   - Records processed: {total_records}")
print(f"   - Data quality issues identified: {abc_count} 'abc' values")
print("   - Ready for Silver layer implementation")

# COMMAND ----------

# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import os
from datetime import datetime

print("=== SILVER LAYER - ASSIGNMENT EVIDENCE ===")
print(f"Execution Time: {datetime.now()}")
print(f"Workspace URL: {spark.conf.get('spark.databricks.workspaceUrl', 'N/A')}")
print(f"Spark Version: {spark.version}")

# COMMAND ----------

# Verify Bronze layer data exists
try:
    bronze_count = spark.sql("SELECT COUNT(*) as count FROM material_master_bronze").collect()[0]['count']
    print(f"✅ Bronze layer verified: {bronze_count} records available")
except:
    print("❌ Bronze layer not found. Please run Bronze layer first.")
    dbutils.notebook.exit("Bronze layer required")

# COMMAND ----------

# Silver Layer Data Cleaning and Standardization
print("=== STARTING SILVER LAYER PROCESSING ===")

class MaterialMasterSilverPipeline:
    
    def __init__(self, spark):
        self.spark = spark
        
    def read_from_bronze(self):
        """Read data from Bronze temporary view"""
        bronze_df = spark.sql("SELECT * FROM material_master_bronze")
        print(f"📖 Read {bronze_df.count()} records from Bronze layer")
        return bronze_df
    
    def clean_and_standardize_data(self, df):
        """Apply comprehensive data cleaning rules"""
        print("🧹 Applying data cleaning and standardization...")
        
        cleaned_df = df \
            .withColumn("material_id", upper(trim(col("material_id")))) \
            .withColumn("material_name", trim(col("material_name"))) \
            .withColumn("category", upper(trim(col("category")))) \
            .withColumn("sub_category", upper(trim(col("sub_category")))) \
            .withColumn("uom", upper(trim(col("uom")))) \
            .withColumn("unit_cost_cleaned", 
                       when(col("unit_cost") == "abc", None)
                       .otherwise(col("unit_cost").cast("double"))) \
            .withColumn("supplier_name", trim(col("supplier_name"))) \
            .withColumn("country", upper(trim(col("country")))) \
            .withColumn("plant", upper(trim(col("plant")))) \
            .withColumn("status_standardized", 
                       when(upper(col("status")) == "ACTIVE", "Active")
                       .when(upper(col("status")) == "INACTIVE", "Inactive") 
                       .when(upper(col("status")) == "OBSOLETE", "Obsolete")
                       .otherwise("Unknown")) \
            .withColumn("last_updated_date", to_date(col("last_updated"), "yyyy-MM-dd")) \
            .withColumn("lead_time_days_int", col("lead_time_days").cast("integer")) \
            .withColumn("safety_stock_int", col("safety_stock").cast("integer")) \
            .withColumn("reorder_level_int", col("reorder_level").cast("integer"))
        
        return cleaned_df
    
    def calculate_data_quality_score(self, df):
        """Calculate data quality score for each record"""
        print("📊 Calculating data quality scores...")
        
        quality_df = df \
            .withColumn("quality_material_id", 
                       when(col("material_id").isNull(), 0.0).otherwise(1.0)) \
            .withColumn("quality_material_name",
                       when(col("material_name").isNull(), 0.0).otherwise(1.0)) \
            .withColumn("quality_unit_cost",
                       when(col("unit_cost_cleaned").isNull(), 0.0).otherwise(1.0)) \
            .withColumn("quality_supplier",
                       when(col("supplier_name").isNull(), 0.5).otherwise(1.0)) \
            .withColumn("quality_dates",
                       when(col("last_updated_date").isNull(), 0.5).otherwise(1.0))
        
        # Calculate weighted quality score
        quality_df = quality_df \
            .withColumn("data_quality_score",
                       round(((col("quality_material_id") * 0.25) +
                              (col("quality_material_name") * 0.25) +
                              (col("quality_unit_cost") * 0.30) +
                              (col("quality_supplier") * 0.10) +
                              (col("quality_dates") * 0.10)), 2)) \
            .withColumn("quality_grade",
                       when(col("data_quality_score") >= 0.9, "HIGH")
                       .when(col("data_quality_score") >= 0.7, "MEDIUM")
                       .when(col("data_quality_score") >= 0.5, "LOW")
                       .otherwise("POOR"))
        
        # Drop intermediate quality columns
        final_cols = [col for col in quality_df.columns if not col.startswith("quality_")]
        final_cols.extend(["data_quality_score", "quality_grade"])
        
        return quality_df.select(*final_cols)
    
    def apply_data_expectations(self, df):
        """Apply data expectation: Material ID should not be null"""
        print("✅ Applying data expectation: Material ID validation...")
        
        # Data Expectation: Material ID should not be null or empty
        expectation_df = df.withColumn("expectation_material_id_valid",
                                     when((col("material_id").isNull()) | 
                                          (trim(col("material_id")) == ""), 
                                          False).otherwise(True))
        
        # Calculate expectation statistics
        total_records = expectation_df.count()
        failed_records = expectation_df.filter(col("expectation_material_id_valid") == False).count()
        success_rate = ((total_records - failed_records) / total_records) * 100 if total_records > 0 else 0
        
        print(f"📋 DATA EXPECTATION RESULTS:")
        print(f"   Total records: {total_records}")
        print(f"   Failed expectation: {failed_records}")
        print(f"   Success rate: {success_rate:.2f}%")
        
        return expectation_df
    
    def filter_valid_records(self, df):
        """Filter records based on business rules"""
        print("🔍 Filtering valid records...")
        
        # Business rule: Keep records with quality score >= 0.5
        valid_df = df.filter(col("data_quality_score") >= 0.5)
        
        original_count = df.count()
        valid_count = valid_df.count()
        filtered_count = original_count - valid_count
        
        print(f"   Original records: {original_count}")
        print(f"   Valid records: {valid_count}")
        print(f"   Filtered out: {filtered_count}")
        
        return valid_df
    
    def add_business_enrichment(self, df):
        """Add business logic and enrichment columns"""
        print("💼 Adding business enrichment...")
        
        enriched_df = df \
            .withColumn("material_group",
                       when(col("category") == "RAW MATERIAL", "Materials")
                       .when(col("category") == "COMPONENT", "Components")
                       .when(col("category") == "CONSUMABLE", "Consumables")
                       .when(col("category") == "PACKAGING", "Packaging")
                       .otherwise("Other")) \
            .withColumn("cost_category",
                       when(col("unit_cost_cleaned") <= 50, "Low Cost")
                       .when(col("unit_cost_cleaned") <= 200, "Medium Cost")
                       .when(col("unit_cost_cleaned") <= 400, "High Cost")
                       .when(col("unit_cost_cleaned") > 400, "Premium Cost")
                       .otherwise("Unknown")) \
            .withColumn("reorder_urgency",
                       when(col("lead_time_days_int") <= 5, "Urgent")
                       .when(col("lead_time_days_int") <= 15, "Normal")
                       .when(col("lead_time_days_int") > 15, "Planned")
                       .otherwise("Unknown")) \
            .withColumn("silver_processed_timestamp", current_timestamp()) \
            .withColumn("data_lineage", lit("Bronze -> Silver"))
        
        return enriched_df
    
    def run_silver_pipeline(self):
        """Execute complete Silver layer pipeline"""
        print("=" * 70)
        print("🚀 SILVER LAYER PIPELINE - EXECUTION STARTED")  
        print("=" * 70)
        
        start_time = datetime.now()
        
        # Step 1: Read from Bronze
        bronze_df = self.read_from_bronze()
        
        # Step 2: Clean and standardize
        cleaned_df = self.clean_and_standardize_data(bronze_df)
        
        # Step 3: Calculate quality scores
        quality_df = self.calculate_data_quality_score(cleaned_df)
        
        # Step 4: Apply data expectations
        expectation_df = self.apply_data_expectations(quality_df)
        
        # Step 5: Filter valid records
        valid_df = self.filter_valid_records(expectation_df)
        
        # Step 6: Add business enrichment
        final_df = self.add_business_enrichment(valid_df)
        
        # Step 7: Create Silver temporary view
        final_df.createOrReplaceTempView("material_master_silver")
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        print("=" * 70)
        print("🎉 SILVER LAYER PIPELINE - EXECUTION COMPLETED")
        print("=" * 70)
        print(f"⏱️  Processing time: {duration:.2f} seconds")
        print(f"📊 Silver records: {final_df.count()}")
        print("📋 Silver table: material_master_silver")
        
        return True

# COMMAND ----------

# Execute Silver Pipeline
silver_pipeline = MaterialMasterSilverPipeline(spark)
pipeline_success = silver_pipeline.run_silver_pipeline()

if pipeline_success:
    print("\n✅ SILVER LAYER IMPLEMENTATION SUCCESSFUL!")
else:
    print("\n❌ SILVER LAYER IMPLEMENTATION FAILED!")

# COMMAND ----------

# Silver Layer Data Quality Analysis
print("=== SILVER LAYER DATA QUALITY ANALYSIS ===")

# Basic statistics
silver_stats = spark.sql("""
    SELECT 
        COUNT(*) as total_records,
        COUNT(DISTINCT material_id) as unique_materials,
        AVG(data_quality_score) as avg_quality_score,
        MIN(data_quality_score) as min_quality_score,
        MAX(data_quality_score) as max_quality_score
    FROM material_master_silver
""").collect()[0]

print(f"Total Records: {silver_stats['total_records']}")
print(f"Unique Materials: {silver_stats['unique_materials']}")
print(f"Average Quality Score: {silver_stats['avg_quality_score']:.3f}")
print(f"Quality Score Range: {silver_stats['min_quality_score']} - {silver_stats['max_quality_score']}")

# COMMAND ----------

# Quality Grade Distribution
print("\n📊 Quality Grade Distribution:")
spark.sql("""
    SELECT quality_grade, COUNT(*) as count, 
           ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
    FROM material_master_silver 
    GROUP BY quality_grade 
    ORDER BY count DESC
""").show()

# COMMAND ----------

# Data Cleaning Results
print("🧹 Data Cleaning Results:")

# Unit cost cleaning
unit_cost_stats = spark.sql("""
    SELECT 
        COUNT(*) as total_records,
        COUNT(unit_cost_cleaned) as valid_unit_costs,
        COUNT(*) - COUNT(unit_cost_cleaned) as invalid_unit_costs
    FROM material_master_silver
""").collect()[0]

print(f"Unit Cost - Valid: {unit_cost_stats['valid_unit_costs']}, Invalid: {unit_cost_stats['invalid_unit_costs']}")

# Status standardization
print("\nStatus Standardization:")
spark.sql("""
    SELECT status_standardized, COUNT(*) as count 
    FROM material_master_silver 
    GROUP BY status_standardized 
    ORDER BY count DESC
""").show()

# COMMAND ----------

# Business Enrichment Analysis
print("💼 Business Enrichment Analysis:")

print("\nMaterial Group Distribution:")
spark.sql("""
    SELECT material_group, COUNT(*) as count 
    FROM material_master_silver 
    GROUP BY material_group 
    ORDER BY count DESC
""").show()

print("Cost Category Distribution:")
spark.sql("""
    SELECT cost_category, COUNT(*) as count,
           ROUND(AVG(unit_cost_cleaned), 2) as avg_cost
    FROM material_master_silver 
    GROUP BY cost_category 
    ORDER BY avg_cost
""").show()

# COMMAND ----------

# Sample of Silver Data
print("=== SILVER LAYER SAMPLE DATA ===")
spark.sql("""
    SELECT 
        material_id,
        material_name,
        unit_cost_cleaned,
        status_standardized,
        quality_grade,
        material_group,
        cost_category,
        data_quality_score
    FROM material_master_silver 
    ORDER BY data_quality_score DESC
    LIMIT 10
""").show(truncate=False)

# COMMAND ----------

# Final Summary
print("=" * 70)
print("SILVER LAYER IMPLEMENTATION SUMMARY")
print("=" * 70)
print("✅ Data cleaning and standardization completed")
print("✅ Unit cost 'abc' values handled (converted to null)")
print("✅ Data types converted (strings to appropriate types)")
print("✅ Status values standardized")
print("✅ Data quality scoring implemented")
print("✅ Data expectation applied (Material ID validation)")
print("✅ Business enrichment columns added")
print("✅ Invalid records filtered based on quality threshold")
print("\n📋 ASSIGNMENT EVIDENCE:")
print(f"   - Data Expectation: Material ID validation with success rate tracking")
print(f"   - Data Cleaning: Fixed 'abc' unit costs, standardized status values")
print(f"   - Type Conversion: String to numeric/date conversions")
print(f"   - Quality Filtering: Records with quality score >= 0.5")
print("   - Silver layer ready for Gold layer or analytics")

print(f"\n🎉 PIPELINE COMPLETE - Bronze → Silver transformation successful!")