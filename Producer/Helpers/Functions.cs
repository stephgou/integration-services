using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System.Reflection;
using System.Runtime.InteropServices;

namespace Producer.Helpers
{
    public static class Functions
    {

        [DllImport("kernel32", SetLastError = true)]
        private static extern IntPtr LoadLibrary(string lpFileName);

        [FunctionName("Helper")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req,
            ILogger log)
        {
            log.LogInformation("C# HTTP trigger function processed a request.");

            string name = req.Query["name"];

            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            dynamic data = JsonConvert.DeserializeObject(requestBody);
            name = name ?? data?.name;


            var is64 = IntPtr.Size == 8;
            try
            {
                var baseUri = new Uri(Assembly.GetExecutingAssembly().GetName().CodeBase);
                log.LogInformation(baseUri.LocalPath);

                var baseDirectory = Path.GetDirectoryName(baseUri.LocalPath);
                log.LogInformation(baseDirectory);

                var parentDirectory = Helper.Left(baseDirectory, baseDirectory.Length - 3);
                var resolvingDll = Path.Combine(parentDirectory, is64 ? "librdkafka\\x64\\librdkafka.dll" : "librdkafka\\x86\\librdkafka.dll");
                log.LogInformation(resolvingDll);
                LoadLibrary(resolvingDll);

                LoadLibrary(Path.Combine(baseDirectory, is64 ? "librdkafka\\x64\\zlib.dll" : "librdkafka\\x86\\zlib.dll"));
                LoadLibrary(Path.Combine(baseDirectory, is64 ? "librdkafka\\x64\\librdkafkacpp.dll" : "librdkafka\\x86\\librdkafkacpp.dll"));
                LoadLibrary(Path.Combine(baseDirectory, is64 ? "librdkafka\\x64\\libzstd.dll" : "librdkafka\\x86\\libzstd.dll"));
                LoadLibrary(Path.Combine(baseDirectory, is64 ? "librdkafka\\x64\\msvcp120.dll" : "librdkafka\\x86\\msvcp120.dll"));
                LoadLibrary(Path.Combine(baseDirectory, is64 ? "librdkafka\\x64\\msvcr120.dll" : "librdkafka\\x86\\msvcr120.dll"));
            }
            catch (Exception) { }

            string responseMessage = string.IsNullOrEmpty(name)
                ? "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response."
                : $"Hello, {name}. This HTTP triggered function executed successfully.";

            return new OkObjectResult(responseMessage);
        }

    }
}
