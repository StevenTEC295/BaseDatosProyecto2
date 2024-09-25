using Entities;
using QueryProcessor.Exceptions;
using QueryProcessor.Operations;
using StoreDataManager;
using System.ComponentModel.DataAnnotations.Schema;

namespace QueryProcessor
{
    public class SQLQueryProcessor
    {
        public static OperationStatus Execute(string sentence)
        {
            /// The following is example code. Parser should be called
            /// on the sentence to understand and process what is requested
            if (sentence.StartsWith("CREATE TABLE"))
            {
                return new CreateTable().Execute();
            }
            if (sentence.StartsWith("CREATE DATABASE"))
            {
                string[] QueryList = sentence.Split(' ');
                string DBName = QueryList[2];
                return new CreateDatabase(DBName).Execute();
            }
            if (sentence.StartsWith("SELECT"))
            {
                return new Select().Execute();
            }
            else
            {
                throw new UnknownSQLSentenceException();
            }
        }
    }
}
