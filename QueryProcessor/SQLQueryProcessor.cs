using Entities;
using QueryProcessor.Exceptions;
using QueryProcessor.Operations;

namespace QueryProcessor
{
    public class SQLQueryProcessor
    {
        private static string[] ParseColumnsToSelect(string selectPart)
            {
                // Remover "SELECT" y "FROM" para quedarse solo con las columnas
                const string selectKeyword = "SELECT";
                const string fromKeyword = "FROM";
                var columnsPart = selectPart.Substring(selectKeyword.Length, selectPart.IndexOf(fromKeyword) - selectKeyword.Length).Trim();

                // Si es un asterisco (*), seleccionar todas las columnas
                if (columnsPart == "*")
                {
                    return new string[] { "*" }; // Indica que se seleccionan todas las columnas
                }

                // Si no, separar por comas las columnas específicas
                return columnsPart.Split(new[] { ',' }, StringSplitOptions.RemoveEmptyEntries)
                                .Select(c => c.Trim())
                                .ToArray();
            }

        private static string ExtractTableNameFromSelect(string sentence)
        {
            const string fromKeyword = "FROM";
            var fromIndex = sentence.IndexOf(fromKeyword) + fromKeyword.Length;
            return sentence.Substring(fromIndex).Trim().Split(' ')[0]; // Extraer el nombre de la tabla después de "FROM"
        }
        public static (OperationStatus Status, string Data) Execute(string sentence)
        {
            if (sentence.StartsWith("CREATE TABLE"))
            {
                var createTable = new CreateTable(); //Para todas las operaciones se crea una instancia de la operación a realizar.
                var status = createTable.Execute(sentence);
                return (status, string.Empty);
            }
            if (sentence.StartsWith("INSERT INTO"))
            {
                var insert = new Insert();
                var status = insert.Execute(sentence);
                return (status, string.Empty);
            }
            if (sentence.StartsWith("SET"))
            {
                const string SetKeyWord = "SET";
                var DataBaseToSet = sentence.Substring(SetKeyWord.Length).Trim(); //De esta manera se puede obtener el nombre de la base de datos a settear.

                if (string.IsNullOrWhiteSpace(DataBaseToSet)) //En caso de que sea nula o solo tiene espacios
                {
                    throw new InvalidOperationException("Debe ingresar un nombre de una BD para settear y realizar consultas");
                }

                var result = new Set().Execute(DataBaseToSet);//Pasamos el nombre de la base de datos a settear como ruta para crear tablas
                return (result, string.Empty);//Devolvemos éxito.
            }     

            // Implementación de la sentencia SELECT
            if (sentence.StartsWith("SELECT"))
            {
                string[] columnsToSelect;
                string tableName;
                string whereClause = null;

                //Detectar si hay un Where
                if (sentence.Contains("WHERE"))
                {
                    var parts = sentence.Split(new[] { "WHERE" }, StringSplitOptions.None);
                    whereClause = parts[1].Trim();
                    var selectPart = parts[0].Trim();
                    // Parsear la parte del SELECT
                    columnsToSelect = ParseColumnsToSelect(selectPart);
                    tableName = ExtractTableNameFromSelect(selectPart);
                }
                else
                {
                    // Si no hay WHERE, solo parsear el SELECT y FROM
                    columnsToSelect = ParseColumnsToSelect(sentence);
                    tableName = ExtractTableNameFromSelect(sentence);
                }

                const string selectDataBaseKeyWord = "SELECT * FROM";
                var DataBaseToSelect = sentence.Substring(selectDataBaseKeyWord.Length).Trim(); //Igual, eliminamos la pabra clave.

               if (string.IsNullOrWhiteSpace(tableName))
                {
                    throw new InvalidOperationException("Debe ingresar un nombre de tabla para seleccionar.");
                }

                var result = new Select().Execute(tableName, columnsToSelect, whereClause); // Pasar columnas, tabla y cláusula WHERE
                return result;
            }
            //Auxiliares de Select
            
            if (sentence.StartsWith("CREATE DATABASE"))
            {
                const string createDatabaseKeyword = "CREATE DATABASE";
                var databaseName = sentence.Substring(createDatabaseKeyword.Length).Trim(); //De igual forma substraemos el nombre de la base de datos a crear.

                if (string.IsNullOrWhiteSpace(databaseName)) 
                { 
                    throw new InvalidOperationException("Debe ingresar un nombre para la base de datos, especifíquelo en el archivo de texto");
                }

                var result = new CreateDataBase().Execute(databaseName); //Pasamos dicho nombre.
                return (result, string.Empty);
            }
            // Implementacion del DROP TABLE 
            if (sentence.StartsWith("DROP TABLE"))
            {
                const string dropDatabaseKeyword = "DROP TABLE";
                var tableName = sentence.Substring(dropDatabaseKeyword.Length).Trim(); //De igual forma substraemos el nombre de la base de datos a crear.
                if (string.IsNullOrWhiteSpace(tableName)) 
                { 
                    throw new InvalidOperationException("Debe ingresar un nombre para la base de datos, especifíquelo en el archivo de texto");
                }
                var result = new DropTable().Execute(tableName); //Pasamos dicho nombre.
                return (result, string.Empty);
            }
            if (sentence.StartsWith("CREATE INDEX"))
            {
                // Se parsea la instrucción completa con el objetivo de obtener la información deseada para crear el índice.
                // Ajuste de Split para evitar problemas con los delimitadores
                var parts = sentence.Split(new[] { "CREATE INDEX ", " ON ", "(", ")", " OF TYPE " }, StringSplitOptions.RemoveEmptyEntries);
            
                if (parts.Length != 4)
                {
                    throw new Exception("Error al parsear la instrucción CREATE INDEX. La sintaxis es incorrecta.");
                }
            
                string indexName = parts[0].Trim(); // Aquí se obtiene el nombre del índice
                string tableName = parts[1].Trim(); // Aquí se obtiene el nombre de la tabla
                string columnNameKeyValue = parts[2].Trim(); // Aquí se obtiene la columna clave (debería ser 'ID o primaryKey')
                string indexType = parts[3].Trim(); // Aquí se obtiene el tipo de índice (BTREE o BST)
            
                // Verificar que el tipo de índice sea válido
                if (indexType != "BTREE" && indexType != "BST")
                {
                    throw new Exception("Tipo de índice no válido. Use 'BTREE' o 'BST'.");
                }
            
                //Se pasan todos los datos para verificar si la tabla, columnas etc... existen.
                var result = new CreateIndexes().Execute(indexName, tableName, columnNameKeyValue, indexType);
                return (result, string.Empty); //Se devuelve el resultado de la operación.
            }
            else
            {
                throw new UnknownSQLSentenceException();
            }

            
            }

            

        }
}
