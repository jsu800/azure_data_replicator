using System;
using System.Diagnostics;

namespace WebDataReplication
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("This Replicator copies all tables from source to target Azure Table Storage and Blob Storage");

            StorageAccountMigrator migrator = new StorageAccountMigrator();

            try
            {
                migrator.Start().Wait();
            }
            catch (Exception ex)
            {
                Console.WriteLine("Async Exception :" + ex.Message);
                Debug.WriteLine("Async Exception :" + ex.ToString());
            }

            Console.WriteLine("Exiting");

        }
    }
}