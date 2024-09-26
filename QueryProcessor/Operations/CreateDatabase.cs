using System;

namespace 
{
    internal class CreateDatabase
    {
        internal OperationStatus Execute()
        {
            return Store.GetInstance().CreateDatabase();
        }
    }

}

