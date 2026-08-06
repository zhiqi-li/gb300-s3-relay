from gb300_relay import OpenAI

client = OpenAI()
completion = client.chat.completions.create(
    model="your-model",
    messages=[{"role": "user", "content": "Introduce GB300 in one sentence."}],
    extra_headers={"idempotency-key": "example-chat-001"},
)
print(completion.choices[0].message.content)
