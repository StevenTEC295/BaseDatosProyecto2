function Execute-MyQuery {
    param (
        [Parameter(Mandatory=$true)]
        [string]$QueryFile,  # Ruta del archivo con sentencias SQL
        [Parameter(Mandatory=$true)]
        [string]$IP,         # IP del servidor
        [Parameter(Mandatory=$true)]
        [int]$Port           # Puerto del servidor
    )

    # Validando que el archivo de consulta exista
    if (-not (Test-Path $QueryFile)) {
        Write-Host -ForegroundColor Red "El archivo no existe: $QueryFile"
        return
    }

    # Leyendo las sentencias SQL desde el archivo
    $sqlQueries = Get-Content -Path $QueryFile -Raw
    Write-Host -ForegroundColor Green "Consultas SQL leídas desde el archivo: `n$sqlQueries"

    # Creando el socket para la conexión
    $ipEndPoint = [System.Net.IPEndPoint]::new([System.Net.IPAddress]::Parse($IP), $Port)
    $client = New-Object System.Net.Sockets.Socket($ipEndPoint.AddressFamily, [System.Net.Sockets.SocketType]::Stream, [System.Net.Sockets.ProtocolType]::Tcp)
    
    try {
        # Conectando al servidor
        $client.Connect($ipEndPoint)
        Write-Host "Conectado al servidor en ${IP}:${Port}"

        # Creando el objeto de petición
        $requestObject = [PSCustomObject]@{
            RequestType = 0 
            RequestBody = $sqlQueries
        }

        # Serializando la petición a JSON
        $jsonMessage = ConvertTo-Json -InputObject $requestObject -Compress

        # Enviando la petición al servidor
        Send-Message -client $client -message $jsonMessage

        # Recibiendo la respuesta
        $response = Receive-Message -client $client

        if ($response) {
            # Convertiendo la respuesta de JSON a un objeto de PowerShell
            $responseObject = ConvertFrom-Json -InputObject $response

            # Mostrando la respuesta en formato de tabla
            if ($responseObject -is [System.Collections.IEnumerable]) {
                $responseObject | Format-Table -AutoSize
            } else {
                Write-Output $responseObject
            }
        } else {
            Write-Host -ForegroundColor Red "No se recibió respuesta del servidor."
        }
    } catch {
        Write-Host -ForegroundColor Red "Error al ejecutar la consulta: $_"
    } finally {
        # Cerrando la conexión
        $client.Shutdown([System.Net.Sockets.SocketShutdown]::Both)
        $client.Close()
    }
}


function Send-Message {
    param (
        [Parameter(Mandatory=$true)]
        [pscustomobject]$message,
        [Parameter(Mandatory=$true)]
        [System.Net.Sockets.Socket]$client
    )

    $stream = New-Object System.Net.Sockets.NetworkStream($client)
    $writer = New-Object System.IO.StreamWriter($stream)
    try {
        $writer.WriteLine($message)
        $writer.Flush()  # Asegurar que se envía el mensaje
    }
    finally {
        $writer.Close()
        $stream.Close()
    }
}

function Receive-Message {
    param (
        [System.Net.Sockets.Socket]$client
    )
    $stream = New-Object System.Net.Sockets.NetworkStream($client)
    $reader = New-Object System.IO.StreamReader($stream)
    try {
        return $reader.ReadLine() ?? ""
    }
    finally {
        $reader.Close()
        $stream.Close()
    }
}

