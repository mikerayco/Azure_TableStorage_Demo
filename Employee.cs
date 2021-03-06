using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Table;

namespace TableStorageDemo
{
    public class Employee : TableEntity
    {
        public string Region { get { return this.PartitionKey; } }
        public string Email { get { return this.RowKey; } }
        public string Name { get; set; }
        public string Phone { get; set; }

        public Employee()
        {
        }

        public Employee(string email, string region, string name, string phone = null)
        {
            this.PartitionKey = region;
            this.RowKey = email;
            this.Name = name;
            this.Phone = phone;
        }

        public override string ToString()
        {
            return $"Region(pk): {Region} Email(rk): {Email} Name: {Name} Phone: {Phone}";
        }

    }
}
