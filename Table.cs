using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using System.Collections.Generic;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace TableStorageDemo
{

    public class Table
    {
        private static string _connectionString = Environment.GetEnvironmentVariable("connection_string");

        public static async Task RunTableDemo()
        {

            //table client
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(_connectionString);
            CloudTableClient tableClient = storageAccount.CreateCloudTableClient();

            //create table
            CloudTable table = tableClient.GetTableReference("employees");
            await table.CreateIfNotExistsAsync();

            //add record
            var emp1 = new Employee("steveappleseed@demo.com", "US", "Steve");
            await InsertData(table, emp1);

            //Retrieve records

            List<Employee> employees = await FindEmployeesByName(table, "Steve");
            employees.ForEach(Console.WriteLine);



        }

        public static async Task<List<Employee>> FindEmployeesByName(CloudTable table, string name)
        {

            var filterCodition = TableQuery.GenerateFilterCondition("Name", QueryComparisons.Equal, name);
            var query = new TableQuery<Employee>().Where(filterCodition);
            var results = await table.ExecuteQuerySegmentedAsync(query, null);
            return results.ToList();
        }


        private static async Task InsertData<T>(CloudTable table, T entity) where T : TableEntity
        {
            var insertOperation = TableOperation.Insert(entity);
            await table.ExecuteAsync(insertOperation);
        }
    }
}
