using Entities;
using StoreDataManager;
using System.Text.Json;

namespace QueryProcessor.Operations
{
    internal class Select
    {
        public (OperationStatus Status, string Data) Execute(string NombreDeBDaSeleccionar, string columnName = null, string conditionValue = null, string operatorValue = "==")
        {
            if (string.IsNullOrEmpty(columnName) || string.IsNullOrEmpty(conditionValue))
            {
                // Si no se proporciona la columna o el valor de la condición, ejecutamos un Select general
                return Store.GetInstance().Select(NombreDeBDaSeleccionar);  // Select sin WHERE
            }
            else
            {
                // Si se proporciona una columna y un valor, ejecutamos el Select con WHERE
                return Store.GetInstance().SelectWhere(NombreDeBDaSeleccionar, columnName, conditionValue, operatorValue);
            }
        }
    }
}
