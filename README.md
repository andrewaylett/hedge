# Hedge

As a service client, if a request is taking longer than usual to complete, it may be worth sending a second request to hedge against the first request continuing to take a long time.

This is a simple library
that provides a way to hedge requests when they take longer than expected.
It uses a sliding window algorithm to determine at what point to send a hedging request.

## Usage

Include the library in your dependencies:

<!-- [[[cog
result = sp.run(
    ["./gradlew", "-q", "printCurrentVersion"],
    capture_output=True,
    text=True,
    check=True
)
version = result.stdout.strip()
cog.outl(f"""```groovy
dependencies {{
    implementation 'eu.aylett:hedge:{version}'
}}
```""")
]]] -->
```groovy
dependencies {
    implementation 'eu.aylett:hedge:0.1.1'
}
```
<!-- [[[end]]] -->
