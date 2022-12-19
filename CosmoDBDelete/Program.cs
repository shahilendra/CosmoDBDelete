// See https://aka.ms/new-console-template for more information
using Microsoft.Azure.Cosmos;
using CosmoDBDelete;

Console.WriteLine("Hello, World!");

var cosmoDBRepository = new CosmoDBRepository();
await cosmoDBRepository.RemoveSeries();

Console.ReadLine();
