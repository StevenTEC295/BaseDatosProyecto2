﻿using Entities;
using QueryProcessor.Exceptions;
using QueryProcessor.Operations;

namespace QueryProcessor
{
    public class SQLQueryProcessor
    {
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
                const string selectDataBaseKeyWord = "SELECT * FROM";
                string whereClause = null;
                string columnName = null;
                string conditionValue = null;
                string operatorValue = "==";  // Operador por defecto

                var dataBaseToSelect = sentence.Substring(selectDataBaseKeyWord.Length).Trim();

                // Comprobar si hay una cláusula WHERE
                if (dataBaseToSelect.Contains("WHERE"))
                {
                    // Dividir la sentencia para obtener la base de datos y la cláusula WHERE
                    var parts = dataBaseToSelect.Split(new[] { "WHERE" }, StringSplitOptions.RemoveEmptyEntries);
                    dataBaseToSelect = parts[0].Trim(); // Nombre de la base de datos o tabla
                    whereClause = parts[1].Trim(); // La cláusula WHERE

                    // Procesar la cláusula WHERE (asumimos que el formato es `columna operador valor`)
                    var whereParts = whereClause.Split(new[] { ' ', '=' }, StringSplitOptions.RemoveEmptyEntries);
                    if (whereParts.Length >= 2)
                    {
                        columnName = whereParts[0].Trim();  // Nombre de la columna
                        conditionValue = whereParts[1].Trim();  // Valor de la condición

                        // Si hay un operador (e.g., <, >, <=, >=, !=)
                        if (whereParts.Length == 3)
                        {
                            operatorValue = whereParts[1].Trim();  // Operador como <, >, !=, etc.
                            conditionValue = whereParts[2].Trim(); // Valor de la condición
                        }
                    }
                    else
                    {
                        throw new InvalidOperationException("Formato inválido en la cláusula WHERE.");
                    }
                }

                if (string.IsNullOrWhiteSpace(dataBaseToSelect))
                {
                    throw new InvalidOperationException("Debe ingresar un nombre de una tabla para seleccionar.");
                }

                // Llamar a Select con o sin WHERE según corresponda
                var result = new Select().Execute(dataBaseToSelect, columnName, conditionValue, operatorValue);
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
            if (sentence.StartsWith("UPDATE"))
            {
                // Split the sentence into the relevant parts
                var parts = sentence.Split(new[] { "UPDATE ", " SET ", " WHERE " }, StringSplitOptions.RemoveEmptyEntries);

                if (parts.Length != 3)
                {
                    throw new Exception("Error al parsear la sentencia UPDATE. La sintaxis es incorrecta.");
                }
                // Extract table name
                string tableName = parts[0].Trim();

                // Split the SET clause into column and value
                var setClause = parts[1].Split(new[] { " = " }, StringSplitOptions.RemoveEmptyEntries);
                if (setClause.Length != 2)
                {
                    throw new Exception("Error al parsear la cláusula SET.");
                }
                string columnToUpdate = setClause[0].Trim();
                string newValue = setClause[1].Trim().Trim('"');  // Remove extra quotes around the value

                // Split the WHERE clause into column and value
                var whereClause = parts[2].Split(new[] { " == " }, StringSplitOptions.RemoveEmptyEntries);
                if (whereClause.Length != 2)
                {
                    throw new Exception("Error al parsear la cláusula WHERE.");
                }
                string whereColumn = whereClause[0].Trim();
                string whereValue = whereClause[1].Trim();

                // Now you have all the parts you need
                Console.WriteLine($"Tabla: {tableName}, Columna a actualizar: {columnToUpdate}, Nuevo valor: {newValue}");
                Console.WriteLine($"Columna WHERE: {whereColumn}, Valor WHERE: {whereValue}");
                Console.WriteLine($"Valores solos para verlos: {tableName},{columnToUpdate},{newValue},{whereColumn},{whereValue}");

                // Puedes llamar a la lógica de la operación Update desde aquí
                var result = new Update().Execute(tableName, columnToUpdate, newValue, whereColumn, whereValue);
                return (result, string.Empty);
            }

            if (sentence.StartsWith("DELETE"))
            {
                const string deleteKeyword = "DELETE FROM";
                string whereClause = null;
                string columnName = null;
                string conditionValue = null;

                var tableToDeleteFrom = sentence.Substring(deleteKeyword.Length).Trim();

                // Comprobar si hay una cláusula WHERE
                if (tableToDeleteFrom.Contains("WHERE"))
                {
                    
                    var parts = tableToDeleteFrom.Split(new[] { "WHERE" }, StringSplitOptions.RemoveEmptyEntries);
                    if (parts.Length < 2){
                        throw new InvalidOperationException("La consulta DELETE no tiene una cláusula WHERE válida.");

                    }
                    
                    tableToDeleteFrom = parts[0].Trim();  // Nombre de la tabla
                    whereClause = parts[1].Trim();  // La cláusula WHERE

                    // Procesar la cláusula WHERE
                    var whereParts = whereClause.Split(new[] { '=' }, StringSplitOptions.RemoveEmptyEntries);
                    if (whereParts.Length == 2)
                    {
                        columnName = whereParts[0].Trim();  // Nombre de la columna
                        conditionValue = whereParts[1].Trim();  // Valor de la condición
                    }
                    else
                    {
                        throw new InvalidOperationException("Formato inválido en la cláusula WHERE.");
                    }
                }

                if (string.IsNullOrWhiteSpace(tableToDeleteFrom))
                {
                    throw new InvalidOperationException("Debe ingresar un nombre de una tabla para eliminar.");
                }

                // Ejecutamos la operación DELETE
                var result = new Delete().Execute(tableToDeleteFrom, columnName, conditionValue);
                return result;
            }

            else
{
    throw new InvalidOperationException("La consulta DELETE no contiene una cláusula WHERE.");
}


            
            }

            

        }
}
