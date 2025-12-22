Write-Host "Stopping existing services on ports 8000 and 8501..."

Get-NetTCPConnection -LocalPort 8000 -ErrorAction SilentlyContinue |
    ForEach-Object { Stop-Process -Id $_.OwningProcess -Force }

Get-NetTCPConnection -LocalPort 8501 -ErrorAction SilentlyContinue |
    ForEach-Object { Stop-Process -Id $_.OwningProcess -Force }

Start-Sleep -Seconds 2

Write-Host "Starting LlamaCPP Server..."
Start-Process powershell -ArgumentList `
    "-NoExit", `
    "-Command", `
    "conda activate emerson; uvicorn LlamaCPPServer.server:app --host 127.0.0.1 --port 8000" `
    -WindowStyle Minimized

Start-Sleep -Seconds 5

Write-Host "Starting Streamlit App..."
Start-Process powershell -ArgumentList `
    "-NoExit", `
    "-Command", `
    "conda activate emerson; streamlit run app_llama.py" `
    -WindowStyle Normal

Write-Host "All services started."
