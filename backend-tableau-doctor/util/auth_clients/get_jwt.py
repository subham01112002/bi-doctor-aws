import datetime
import uuid
import jwt


secretId = "dfc4b32c-6624-4516-8fb3-457725a28d6a"
secretValue = "hHTHdubZxJzvWKHTErxiTCB8TNVLWjBxBMx5B2NIs/0="
clientId = "333b9ed3-0f3c-492a-9fa3-262c34cdbc00"
username = "rahul.neogi@exavalu.com" #Make it dynamic per username
tokenExpiryInMinutes = 1  

# manage scope create/read/run/update/download/delete access
# Example scopes:
scopes = [
    "tableau:views:embed",
    "tableau:views:embed_authoring",
    "tableau:insights:embed",
]

kid = secretId
iss = clientId
sub = username
aud = "tableau"
exp = datetime.datetime.now(datetime.UTC) + datetime.timedelta(minutes=tokenExpiryInMinutes)
jti = str(uuid.uuid4())
scp = scopes

userAttributes = {
  
}

payload = {
    "iss": clientId,
    "exp": exp,
    "jti": jti,
    "aud": aud,
    "sub": sub,
    "scp": scp,
} | userAttributes


def getJwt():
    token = jwt.encode(
        payload,
        secretValue,
        algorithm="HS256",
        headers={
            "kid": kid,
            "iss": iss,
        },
    )

    return token


if __name__ == "__main__":
    token = getJwt()
    print(token)