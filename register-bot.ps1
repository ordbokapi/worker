<#
    A PowerShell script to register a new Matrix user/bot with:
      - Username/Password
      - Optional email verification (threepid)
      - Optional phone verification (MSISDN)
      - Accept terms & conditions
      - Solve reCAPTCHA
    On completion, displays an access token.
#>

# Utility: a random client_secret generator for 3PID flows. (Email, phone) Must be unique per session
function New-ClientSecret {
    [guid]::NewGuid().ToString()
}

# Utility: Convert SecureString => plain text
function UnsecureString {
    param(
        [Parameter(Mandatory = $true)]
        [System.Security.SecureString]$Secure
    )
    return [Runtime.InteropServices.Marshal]::PtrToStringAuto(
        [Runtime.InteropServices.Marshal]::SecureStringToBSTR($Secure)
    )
}

# Utility: Hide sensitive text from output
function Hide-SensitiveText {
    param (
        [string]$Text,
        [string[]]$SensitiveText
    )

    # Replace each instance of sensitive text with a placeholder
    if ($SensitiveText) {
        foreach ($sensitive in $SensitiveText) {
            $escapedSensitive = [regex]::Escape($sensitive)
            $Text = $Text -replace $escapedSensitive, "********"
        }
    }

    return $Text
}

# Function to send POST requests with JSON body.
function Send-MatrixRequest {
    param (
        [string]$Url,
        [hashtable]$Body,
        [string[]]$SensitiveText
    )

    $JsonBody = $Body | ConvertTo-Json -Depth 10 -Compress

    Write-Host "`nPOST $Url" -ForegroundColor Gray

    # Uncomment to see the request body for debugging
    # $SafeBody = Hide-SensitiveText -Text $JsonBody -SensitiveText $SensitiveText
    # Write-Host "Request Body: $SafeBody" -ForegroundColor DarkGray

    try {
        # Capture the raw response so we can see status code & parse content ourselves.
        $Response = Invoke-WebRequest -Uri $Url -Method Post -ContentType "application/json" `
            -Body $JsonBody -SkipHttpErrorCheck
    }
    catch {
        Write-Error "Error: $($_.Exception.Message)"
        exit 1
    }

    if ($Response.Content) {
        $Json = $Response.Content | ConvertFrom-Json
    }
    else {
        $Json = [ordered]@{}  # empty
    }

    # Uncomment to see the response body for debugging
    # $SafeResponse = Hide-SensitiveText -Text $Response.Content -SensitiveText $SensitiveText
    # Write-Host "Response: $($Response.StatusCode) $SafeResponse" -ForegroundColor DarkGray

    # Just store the numeric status code in the object for convenience
    $Json | Add-Member -NotePropertyName "HttpStatus" -NotePropertyValue $($Response.StatusCode) -Force

    return $Json
}

# --------------------------------------------------------------------------------------------------
# Step 1: Ask for homeserver URL, username, and password
# --------------------------------------------------------------------------------------------------

# Prompt user for the homeserver URL, default matrix.org
$ServerUrl = Read-Host "Enter the Matrix server base URL (default: https://matrix.org)"
if (-not $ServerUrl) {
    $ServerUrl = "https://matrix.org"
}

# The registration endpoint (r0 or v3, they're mostly interchangeable for basic usage):
$RegistrationUrl = "$ServerUrl/_matrix/client/r0/register"

Write-Host "You are about to register a new user on: $ServerUrl" -ForegroundColor Cyan

$Username = Read-Host "Enter a username for the bot"

while ($true) {
    $PasswordSecure = Read-Host -AsSecureString "Enter a password for the bot"
    $UnsecurePassword = UnsecureString $PasswordSecure

    # Have user confirm password.
    $PasswordConfirmSecure = Read-Host -AsSecureString "Please confirm the password"

    if ($UnsecurePassword -ne (UnsecureString $PasswordConfirmSecure)) {
        Write-Host "Passwords do not match. Please try again." -ForegroundColor Red
        continue
    }

    break
}

# --------------------------------------------------------------------------------------------------
# Step 2: Attempt to start registration by sending username/password in the body.
# --------------------------------------------------------------------------------------------------

# Expect 401 plus UI auth data if the server requires additional steps.
$InitResponse = Send-MatrixRequest -Url $RegistrationUrl -SensitiveText @($UnsecurePassword) -Body @{
    username                    = $Username
    password                    = $UnsecurePassword
    device_id                   = "bot_device"
    initial_device_display_name = "Bot Device"
    # Set this to $true if you want to do a separate /login at the end.
    # If false, the server might directly return an access_token once all flows are complete.
    inhibit_login               = $false
}

if ($InitResponse.HttpStatus -eq 200 -and $InitResponse.user_id -and $InitResponse.access_token) {
    Write-Host "`nRegistration completed in one step! (No extra UI auth required.)" -ForegroundColor Green
    Write-Host "User ID: $($InitResponse.user_id)"
    Write-Host "Access Token: $($InitResponse.access_token)"
    return
}

if (-not $InitResponse.session) {
    Write-Host "`nCould not initiate registration properly. Server response:" -ForegroundColor Red
    $InitResponse | ConvertTo-Json -Depth 10
    exit 1
}

$Session = $InitResponse.session
Write-Host "`nRegistration session: $Session" -ForegroundColor Yellow

# Track the current UI auth response in $CurrentResponse so we can see which
# flows are completed and which are still required.
$CurrentResponse = $InitResponse

# Helper function to see if a stage is completed.
function StageCompleted($Response, $StageName) {
    return $Response.completed -and ($Response.completed -contains $StageName)
}

# Helper function to see if the server's flows/params mention a particular stage is needed.
function StageMentioned($Response, $StageName) {
    # If it appears in any flow that is not fully completed yet, or is in params
    # with some additional data. Usually it's safe to check:
    if ($Response.params.$StageName) { return $true }
    # Or check flows array. A bit more complicated, but let's keep it simple:
    if ($Response.flows) {
        foreach ($flow in $Response.flows) {
            if ($flow.stages -contains $StageName) { return $true }
        }
    }
    return $false
}

# --------------------------------------------------------------------------------------------------
# Step 3: Loop through the registration stages until we get a user_id and access_token.
# --------------------------------------------------------------------------------------------------

# Loop until the server either returns a 200 with user_id or there are no more params to fill.
while ($true) {

    # If the server responded with a user_id + access_token, we are done
    if ($CurrentResponse.user_id -and $CurrentResponse.access_token -and ($CurrentResponse.HttpStatus -eq 200)) {
        Write-Host "`nRegistration COMPLETED successfully!" -ForegroundColor Green
        Write-Host "User ID: $($CurrentResponse.user_id)" -ForegroundColor Cyan
        Write-Host "Access Token: $($CurrentResponse.access_token)" -ForegroundColor Cyan
        Write-Host "Device ID: $($CurrentResponse.device_id)" -ForegroundColor Cyan
        break
    }

    # If server doesn't show any more params but still no success => possibly error
    if (-not $CurrentResponse.params -and -not $CurrentResponse.user_id) {
        Write-Host "`nNo more 'params' but no success either. Possibly an error." -ForegroundColor Red
        $CurrentResponse | ConvertTo-Json -Depth 10
        break
    }

    # -- Terms stage?
    if ((StageMentioned $CurrentResponse "m.login.terms") -and `
            -not (StageCompleted $CurrentResponse "m.login.terms")) {

        Write-Host "`n--- Terms & Conditions required ---" -ForegroundColor Cyan
        $TermData = $CurrentResponse.params."m.login.terms".policies.privacy_policy
        if ($TermData.en.url) {
            Write-Host "Please read and accept the terms here: $($TermData.en.url)" -ForegroundColor DarkYellow
        }
        else {
            Write-Host "The server requires terms acceptance, but no URL was provided." -ForegroundColor DarkYellow
        }
        Read-Host "Press ENTER after you've accepted the Terms"

        $CurrentResponse = Send-MatrixRequest -Url $RegistrationUrl -SensitiveText @($UnsecurePassword) -Body @{
            auth = @{
                type    = "m.login.terms"
                session = $Session
                # user_accepts = $true  # some servers require this boolean
            }
        }
        continue
    }

    # -- Email identity stage?
    # On matrix.org, the flow is either email or dummy. We'll ask user if they want to do email.
    # If yes, we do the requestToken flow. If no, we skip to dummy.
    if ((StageMentioned $CurrentResponse "m.login.email.identity") -and `
            -not (StageCompleted $CurrentResponse "m.login.email.identity")) {

        Write-Host "`n--- Email verification is an option (or required). ---" -ForegroundColor Cyan
        $Answer = Read-Host "Do you want to verify an email address now? [y/N]"
        if ($Answer -match '^(?i)y') {
            # Ask for the email.
            $Email = Read-Host "Enter your email address for verification"
            # We must request a 3PID token from the server.
            # Typically: POST /_matrix/client/r0/register/email/requestToken
            $ClientSecret = New-ClientSecret
            $SendAttempt = 1 # Increment if we retry.

            Write-Host "Requesting email token from the server..." -ForegroundColor Yellow
            $ReqTokenUrl = "$ServerUrl/_matrix/client/r0/register/email/requestToken"
            $ReqTokenResp = Send-MatrixRequest -Url $ReqTokenUrl -SensitiveText @($UnsecurePassword) -Body @{
                client_secret = $ClientSecret
                email         = $Email
                send_attempt  = $SendAttempt
            }

            if ($ReqTokenResp.sid) {
                $SID = $ReqTokenResp.sid
                Write-Host "Email token sent. SID=$SID" -ForegroundColor DarkGreen
                Write-Host "Please check your inbox for a verification link." -ForegroundColor Yellow
                Read-Host "Press ENTER once you have clicked the link in the email."

                # Next step is telling /register we have "m.login.email.identity"
                # If the user hasn't actually verified, the server returns 401 M_UNAUTHORIZED. Loop.
                while ($true) {
                    $CurrentResponse = Send-MatrixRequest -Url $RegistrationUrl -SensitiveText @($UnsecurePassword) -Body @{
                        auth = @{
                            type           = "m.login.email.identity"
                            session        = $Session
                            threepid_creds = @{
                                client_secret = $ClientSecret
                                sid           = $SID
                            }
                        }
                    }

                    if ($CurrentResponse.HttpStatus -eq 401 -and $CurrentResponse.errcode -eq "M_UNAUTHORIZED") {
                        Write-Host "It appears your email is not yet verified. Please click the link in your email." -ForegroundColor Red
                        $Retry = Read-Host "Press [Enter] to try again or type 'skip' to give up."
                        if ($Retry -eq 'skip') { break }
                    }
                    else {
                        # Either we got a 200, or a 401 with completed = [ 'm.login.email.identity' ]
                        # meaning the server accepted it. Break out to the outer loop to handle next step.
                        break
                    }
                }
            }
            else {
                Write-Host "Failed to request email token. Response below:" -ForegroundColor Red
                $ReqTokenResp | ConvertTo-Json -Depth 10
            }
        }
        else {
            Write-Host "Skipping email registration. Will try 'm.login.dummy' if the flow allows." -ForegroundColor DarkGray
        }

        # Continue to next iteration in the loop
        continue
    }

    # -- Phone (MSISDN) stage?
    # If the server's flows mention "m.login.msisdn", handle similarly to email.
    if ((StageMentioned $CurrentResponse "m.login.msisdn") -and `
            -not (StageCompleted $CurrentResponse "m.login.msisdn")) {

        Write-Host "`n--- Phone (MSISDN) verification is an option (or required). ---" -ForegroundColor Cyan
        $Answer = Read-Host "Do you want to verify a phone number now? [y/N]"
        if ($Answer -match '^(?i)y') {
            $Phone = Read-Host "Enter your phone number in international format (e.g. +12025550123)"
            $ClientSecret = New-ClientSecret
            $SendAttempt = 1

            Write-Host "Requesting phone token from the server..." -ForegroundColor Yellow
            $ReqTokenUrl = "$ServerUrl/_matrix/client/r0/register/msisdn/requestToken"
            # Because some servers require country as a separate param,
            # we can parse out from the phone or ask the user:
            # $CountryGuess = "US"
            $ReqTokenResp = Send-MatrixRequest -Url $ReqTokenUrl -SensitiveText @($UnsecurePassword) -Body @{
                client_secret = $ClientSecret
                phone_number  = $Phone
                # country       = $CountryGuess # can omit if number is fully international (e.g. +XXXXXXXX)
                send_attempt  = $SendAttempt
            }

            if ($ReqTokenResp.sid) {
                $SID = $ReqTokenResp.sid
                Write-Host "SMS token sent. SID=$SID" -ForegroundColor DarkGreen
                Write-Host "You should receive an SMS with a code or link." -ForegroundColor Yellow
                if ($ReqTokenResp.submit_url) {
                    Write-Host "The HS might also provide a 'submit_url' you can POST the code to: $($ReqTokenResp.submit_url)"
                }
                # On matrix.org, the user typically receives a link or code.
                # If it's a link, they click it. If it's a code, we do an extra POST to submit_url.
                # Ask them if they want to take the manual approach:
                $Answer2 = Read-Host "Did you get a code that you must type in? [y/N]"
                if ($Answer2 -match '^(?i)y') {
                    $Token = Read-Host "Enter the code you got via SMS"
                    # Do a POST to the submit_url if provided:
                    if ($ReqTokenResp.submit_url) {
                        $SubmitResp = Send-MatrixRequest -Url $ReqTokenResp.submit_url -SensitiveText @($UnsecurePassword) -Body @{
                            client_secret = $ClientSecret
                            sid           = $SID
                            token         = $Token
                        }
                        Write-Host "submit_url response:" ($SubmitResp | ConvertTo-Json -Depth 10)
                        if ($SubmitResp.success -ne $true) {
                            Write-Warning "The server says success=$($SubmitResp.success). Possibly invalid code?"
                        }
                    }
                    else {
                        Write-Warning "No submit_url. This HS might handle code verification differently."
                    }
                }
                else {
                    Write-Host "If it was a link, please click it to verify your phone number." -ForegroundColor DarkCyan
                }

                Read-Host "Press ENTER once you have verified the phone number (clicked link or code)."

                # Now try to do the actual UIA stage for msisdn:
                while ($true) {
                    $CurrentResponse = Send-MatrixRequest -Url $RegistrationUrl -SensitiveText @($UnsecurePassword) -Body @{
                        auth = @{
                            type           = "m.login.msisdn"
                            session        = $Session
                            threepid_creds = @{
                                client_secret = $ClientSecret
                                sid           = $SID
                            }
                        }
                    }

                    if ($CurrentResponse.HttpStatus -eq 401 -and $CurrentResponse.errcode -eq "M_UNAUTHORIZED") {
                        Write-Host "It seems your phone is not yet verified. Try again after verifying." -ForegroundColor Red
                        $Retry = Read-Host "Press [Enter] to retry or type 'skip' to give up."
                        if ($Retry -eq 'skip') { break }
                    }
                    else {
                        break
                    }
                }
            }
            else {
                Write-Host "Failed to request phone token. Response below:" -ForegroundColor Red
                $ReqTokenResp | ConvertTo-Json -Depth 10
            }
        }
        else {
            Write-Host "Skipping phone registration. We'll rely on 'm.login.dummy' if needed." -ForegroundColor DarkGray
        }

        # back to top
        continue
    }

    # -- Dummy stage?
    # On matrix.org, you typically do either email or dummy (and in some flows phone or msisdn).
    # The dummy stage is used to skip optional 3PID binding. If we haven't done an email or phone
    # flow or if the server explicitly requires m.login.dummy, try it:
    if ((StageMentioned $CurrentResponse "m.login.dummy") -and `
            -not (StageCompleted $CurrentResponse "m.login.dummy")) {

        Write-Host "`n-- Completing dummy stage. (Used to skip or finalize 3PID steps.)" -ForegroundColor DarkCyan
        $CurrentResponse = Send-MatrixRequest -Url $RegistrationUrl -SensitiveText @($UnsecurePassword) -Body @{
            auth = @{
                type    = "m.login.dummy"
                session = $Session
            }
        }
        continue
    }

    # -- reCAPTCHA stage?
    if ((StageMentioned $CurrentResponse "m.login.recaptcha") -and `
            -not (StageCompleted $CurrentResponse "m.login.recaptcha")) {

        $PublicKey = $CurrentResponse.params."m.login.recaptcha".public_key
        Write-Host "`n--- reCAPTCHA required ---" -ForegroundColor Cyan
        Write-Host "Public key: $PublicKey"
        Write-Host "Solve the captcha in your browser. Paste the snippet below into the DevTools console on any webpage for the $ServerUrl domain."
        Write-Host ""
        $Snippet = @"
(()=>{
  document.write('<!DOCTYPE html><html><head><script src="https://www.google.com/recaptcha/api.js" async defer></script></head><body style="display:flex;flex-direction:column;align-items:center;font-family:sans-serif"><h1>Complete the CAPTCHA:</h1><div id="g"></div><textarea id="t" rows="4" cols="50"></textarea><div id="n"></div></body></html>');
  const [g,t,n]=["g","t","n"].map((s)=>document.querySelector(`#`${s}`));
  t.onfocus=function(){t.select()};
  const i=setInterval(function(){
    if(typeof grecaptcha!=="undefined"&&grecaptcha.render){
      clearInterval(i);
      grecaptcha.render(g,{
        sitekey:"$PublicKey",
        callback:function(){
          t.value=grecaptcha.getResponse();
          console.log("Copied token:", t.value);
          navigator.clipboard.writeText(t.value);
          n.innerText="CAPTCHA response copied to clipboard.";
          setTimeout(function(){n.innerText=""},3000);
        }
      });
    }
  },100);
})();
"@
        Write-Host $Snippet
        Write-Host ""
        Write-Host "When the box says 'CAPTCHA solved' copy the token from the text area." -ForegroundColor DarkYellow
        $UserToken = Read-Host "Paste the CAPTCHA token here"

        # Post the solution.
        $CurrentResponse = Send-MatrixRequest -Url $RegistrationUrl -SensitiveText @($UnsecurePassword) -Body @{
            auth = @{
                type     = "m.login.recaptcha"
                session  = $Session
                response = $UserToken
            }
        }
        continue
    }

    # If none of the known stages are required or incomplete, break.
    # Possibly we're in a weird custom flow. Break and see the last response.
    Write-Host "`nNo more recognized stages to handle, but still no success or a new step we don't handle."
    break
}

# --------------------------------------------------------------------------------------------------
# Final step: display the user_id and access_token if we got them.
# --------------------------------------------------------------------------------------------------

# Did we get a user_id & access_token from the last step?
if ($CurrentResponse.user_id -and $CurrentResponse.access_token) {
    Write-Host "`nRegistration completed successfully!" -ForegroundColor Green
    Write-Host "User ID: $($CurrentResponse.user_id)" -ForegroundColor Cyan
    Write-Host "Username: $Username" -ForegroundColor Cyan
    Write-Host "Password: ********" -ForegroundColor Cyan
    Write-Host "Access Token: $($CurrentResponse.access_token)" -ForegroundColor Cyan
    Write-Host "Device ID: $($CurrentResponse.device_id)" -ForegroundColor Cyan
    Write-Host "Homeserver: $ServerUrl" -ForegroundColor Cyan
    Write-Host "`nYou can now use this user to interact with the Matrix server." -ForegroundColor Green
}
else {
    Write-Host "`nRegistration not fully completed. Last server response:" -ForegroundColor Red
    $ResponseJson = $CurrentResponse | ConvertTo-Json -Depth 10 -Compress
    $SafeResponse = Hide-SensitiveText -Text $ResponseJson -SensitiveText @($UnsecurePassword)
    Write-Host $SafeResponse

    # Could maybe do an explicit /login if the server demanded that?
    # But typically we won't get an access token if the registration wasn't complete.
    exit 1
}

# If inhibit_login was true earlier, could do a final /login call to get the token:
#
# $LoginResp = Send-MatrixRequest -Url "$ServerUrl/_matrix/client/r0/login" -SensitiveText @($UnsecurePassword) -Body @{
#    type       = "m.login.password"
#    identifier = @{
#        type = "m.id.user"
#        user = $Username
#    }
#    password = $UnsecurePassword
#    device_id= "bot_device"
# }
# Write-Host "Login response: $($LoginResp | ConvertTo-Json -Depth 10)"
