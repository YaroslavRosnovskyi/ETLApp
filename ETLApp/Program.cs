using System.Data;
using System.Globalization;
using CsvHelper;
using CsvHelper.Configuration;
using ETLApp;
using Microsoft.Data.SqlClient;

static class Program
{
    static void Main()
    {
        const string databaseName = "ETLApp";

        // Change connection string if needed
        string connectionString = $"Data Source=(localdb)\\MSSQLLocalDB;Initial Catalog={databaseName};Integrated Security=True;";
        
        string baseDirectory = Directory.GetCurrentDirectory();
        string csvFilePath = Path.Combine(baseDirectory, "..", "..", "..", "sample-cab-data.csv");
        string duplicatesFilePath = Path.Combine(baseDirectory, "..", "..", "..", "duplicates.csv");
        string rowCountFilePath = Path.Combine(baseDirectory, "..", "..", "..", "row-count.txt");
        

        EnsureDatabaseAndTableExist(connectionString, databaseName);

        var records = ReadCsvFile(csvFilePath);
        var validRecords = RemoveCorruptedRows(RemoveDuplicates(records, duplicatesFilePath));

        ProcessData(validRecords);
        BulkInsertToDatabase(validRecords, connectionString);

        int rowCount = GetRowCount(connectionString, databaseName);
        Console.WriteLine($"ETL Process Completed. Number of rows: {rowCount}");
        File.WriteAllText(rowCountFilePath, $"Number of rows: {rowCount}");
    }

    // Ensuring the database and TripData table exist, recreating them for consistency on each program start.  
    static void EnsureDatabaseAndTableExist(string connectionString, string databaseName)
    {
        using var connection = new SqlConnection(connectionString);
        connection.Open();

        var checkDbExistsQuery = $"SELECT database_id FROM sys.databases WHERE Name = '{databaseName}'";
        using var checkCommand = new SqlCommand(checkDbExistsQuery, connection);
        if (checkCommand.ExecuteScalar() == null)
        {
            using var createCommand = new SqlCommand($"CREATE DATABASE {databaseName}", connection);
            createCommand.ExecuteNonQuery();
            Console.WriteLine($"Database '{databaseName}' created.");
        }

        var dbConnectionString = new SqlConnectionStringBuilder(connectionString) { InitialCatalog = databaseName }.ToString();
        using var dbConnection = new SqlConnection(dbConnectionString);
        dbConnection.Open();
        
        var createTableQuery = @"
                                DROP TABLE IF EXISTS TripData;                              
    
                                IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'TripData') AND type = 'U')
                                CREATE TABLE TripData (
                                    tpep_pickup_datetime DATETIME,
                                    tpep_dropoff_datetime DATETIME,
                                    passenger_count INT,
                                    trip_distance DECIMAL(10, 2),
                                    store_and_fwd_flag VARCHAR(3),
                                    PULocationID INT,
                                    DOLocationID INT,
                                    fare_amount DECIMAL(10, 2),
                                    tip_amount DECIMAL(10, 2)
                                );";
        using var createTableCommand = new SqlCommand(createTableQuery, dbConnection);
        createTableCommand.ExecuteNonQuery();
    }

    // Uses CsvHelper for efficient CSV parsing instead of manual string operations or split-based parsing,  
    // as CsvHelper provides robust handling of headers, data types, and missing fields.
    public static IEnumerable<TripRecord> ReadCsvFile(string csvFilePath)
    {
        using var reader = new StreamReader(csvFilePath);
        using var csv = new CsvReader(reader, new CsvConfiguration(CultureInfo.InvariantCulture) { HeaderValidated = null, MissingFieldFound = null });
        csv.Read();
        csv.ReadHeader();

        var records = new List<TripRecord>();
        while (csv.Read())
        {
            if (csv.TryGetField("tpep_pickup_datetime", out DateTime pickupDateTime) &&
                csv.TryGetField("tpep_dropoff_datetime", out DateTime dropoffDateTime) &&
                csv.TryGetField("passenger_count", out int passengerCount) &&
                csv.TryGetField("trip_distance", out decimal tripDistance) &&
                csv.TryGetField("store_and_fwd_flag", out string storeAndFwdFlag) &&
                csv.TryGetField("PULocationID", out int puLocationID) &&
                csv.TryGetField("DOLocationID", out int doLocationID) &&
                csv.TryGetField("fare_amount", out decimal fareAmount) &&
                csv.TryGetField("tip_amount", out decimal tipAmount))
            {
                records.Add(new TripRecord
                {
                    tpep_pickup_datetime = pickupDateTime,
                    tpep_dropoff_datetime = dropoffDateTime,
                    passenger_count = passengerCount,
                    trip_distance = tripDistance,
                    store_and_fwd_flag = storeAndFwdFlag!.Trim(),
                    PULocationID = puLocationID,
                    DOLocationID = doLocationID,
                    fare_amount = fareAmount,
                    tip_amount = tipAmount
                });
            }
        }
        return records;
    }

    public static IEnumerable<TripRecord> RemoveDuplicates(IEnumerable<TripRecord> records, string duplicatesFilePath)
    {
        var uniqueRecords = records
            .GroupBy(r => new { r.tpep_pickup_datetime, r.tpep_dropoff_datetime, r.passenger_count })
            .Select(g => g.First())
            .ToList();

        var duplicates = records.Except(uniqueRecords).ToList();
        WriteDuplicatesToCsv(duplicates, duplicatesFilePath);

        return uniqueRecords;
    }

    public static void WriteDuplicatesToCsv(List<TripRecord> duplicates, string filePath)
    {
        using var writer = new StreamWriter(filePath);
        using var csv = new CsvWriter(writer, new CsvConfiguration(CultureInfo.InvariantCulture));
        csv.WriteRecords(duplicates);
    }

    // Filtered out invalid rows to maintain data accuracy, preventing misleading results in further data manipulations or calculations.
    public static IEnumerable<TripRecord> RemoveCorruptedRows(IEnumerable<TripRecord> records)
    {
        return records.Where(r => r.fare_amount >= 0 && r.tip_amount >= 0).ToList();
    }

    // Used ADO.NET with SqlBulkCopy for high-performance bulk insertion instead of EF Core,  
    // as EF Core is significantly slower for large datasets due to change tracking and individual inserts.  
    static void BulkInsertToDatabase(IEnumerable<TripRecord> records, string connectionString)
    {
        using var connection = new SqlConnection(connectionString);
        connection.Open();
        using var transaction = connection.BeginTransaction();
        using var bulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.TableLock, transaction)
        {
            DestinationTableName = "TripData",
            BatchSize = 10000
        };

        var dataTable = new DataTable();
        dataTable.Columns.AddRange(new[]
        {
            new DataColumn("tpep_pickup_datetime", typeof(DateTime)),
            new DataColumn("tpep_dropoff_datetime", typeof(DateTime)),
            new DataColumn("passenger_count", typeof(int)),
            new DataColumn("trip_distance", typeof(decimal)),
            new DataColumn("store_and_fwd_flag", typeof(string)),
            new DataColumn("PULocationID", typeof(int)),
            new DataColumn("DOLocationID", typeof(int)),
            new DataColumn("fare_amount", typeof(decimal)),
            new DataColumn("tip_amount", typeof(decimal))
        });

        foreach (var record in records)
        {
            dataTable.Rows.Add(
                record.tpep_pickup_datetime,
                record.tpep_dropoff_datetime,
                record.passenger_count,
                record.trip_distance,
                record.store_and_fwd_flag,
                record.PULocationID,
                record.DOLocationID,
                record.fare_amount,
                record.tip_amount);
        }

        bulkCopy.WriteToServer(dataTable);
        transaction.Commit();
    }

    public static int GetRowCount(string connectionString, string databaseName)
    {
        var dbConnectionString = new SqlConnectionStringBuilder(connectionString) { InitialCatalog = databaseName }.ToString();
        using var connection = new SqlConnection(dbConnectionString);
        connection.Open();
        using var command = new SqlCommand("SELECT COUNT(*) FROM TripData", connection);
        return (int)command.ExecuteScalar();
    }

    public static void ProcessData(IEnumerable<TripRecord> records)
    {
        foreach (var record in records)
        {
            record.store_and_fwd_flag = record.store_and_fwd_flag?.Trim()!;

            if (record.store_and_fwd_flag == "N")
            {
                record.store_and_fwd_flag = "No";
            }
            else if (record.store_and_fwd_flag == "Y")
            {
                record.store_and_fwd_flag = "Yes";
            }

            record.tpep_pickup_datetime = TimeZoneInfo.ConvertTimeToUtc(record.tpep_pickup_datetime, TimeZoneInfo.FindSystemTimeZoneById("Eastern Standard Time"));
        }
    }
}