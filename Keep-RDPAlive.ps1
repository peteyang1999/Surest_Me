# Keep-RDPAlive.ps1
# Prevents Remote Desktop session from timing out due to inactivity
# Press Ctrl+C to stop the script

param(
    [int]$IntervalMinutes = 4  # Send keypress every 4 minutes
)

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  Keep RDP Session Alive Script" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Sending Scroll Lock keypress every $IntervalMinutes minute(s)" -ForegroundColor Green
Write-Host "Press Ctrl+C to stop" -ForegroundColor Yellow
Write-Host ""

# Load the Windows Forms assembly for SendKeys
Add-Type -AssemblyName System.Windows.Forms

$iteration = 0

try {
    while ($true) {
        $iteration++
        $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
        
        # Send Scroll Lock key twice (toggle on and off) - doesn't disrupt work
        [System.Windows.Forms.SendKeys]::SendWait("{SCROLLLOCK}")
        Start-Sleep -Milliseconds 100
        [System.Windows.Forms.SendKeys]::SendWait("{SCROLLLOCK}")
        
        Write-Host "[$timestamp] Keep-alive signal sent (iteration $iteration)" -ForegroundColor Green
        
        # Wait for the specified interval
        Start-Sleep -Seconds ($IntervalMinutes * 60)
    }
}
catch {
    Write-Host ""
    Write-Host "Script stopped by user" -ForegroundColor Yellow
}
finally {
    Write-Host ""
    Write-Host "Total keep-alive signals sent: $iteration" -ForegroundColor Cyan
    Write-Host "Script terminated at: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')" -ForegroundColor Cyan
}

