from base64 import b64encode
from pathlib import Path

from gb300_relay import OpenAI


def data_url(path: str, media_type: str) -> str:
    encoded = b64encode(Path(path).read_bytes()).decode("ascii")
    return f"data:{media_type};base64,{encoded}"


client = OpenAI()
completion = client.chat.completions.create(
    model="your-vlm",
    messages=[
        {
            "role": "user",
            "content": [
                {
                    "type": "text",
                    "text": "Summarize the video and explain how it relates to the image.",
                },
                {
                    "type": "image_url",
                    "image_url": {"url": data_url("frame.png", "image/png")},
                },
                {
                    "type": "video_url",
                    "video_url": {"url": data_url("clip.mp4", "video/mp4")},
                },
            ],
        }
    ],
)
print(completion.choices[0].message.content)
