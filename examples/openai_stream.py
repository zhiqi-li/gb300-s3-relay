from gb300_relay import OpenAI

client = OpenAI()
stream = client.chat.completions.create(
    model="your-model",
    messages=[{"role": "user", "content": "Count from one to ten."}],
    stream=True,
)
for event in stream:
    if event.choices and event.choices[0].delta.content:
        print(event.choices[0].delta.content, end="", flush=True)
print()
