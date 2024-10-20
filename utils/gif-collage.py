import sys
import pandas as pd
from PIL import Image, ImageSequence, ImageDraw, ImageFont
from moviepy.editor import (
    ImageSequenceClip,
)  # Import necessary for creating video files
import numpy as np  # Import numpy for array manipulations


def trim_gif(path, start_frame, end_frame):
    gif = Image.open(path)
    frames = [frame.copy() for frame in ImageSequence.Iterator(gif)]
    return frames[start_frame : end_frame + 1]


def create_collage(gifs, gif_names, width, height):
    collage_frames = []
    num_frames = min(
        len(gif) for gif in gifs
    )  # Correctly calculate the minimum number of frames

    for frame_index in range(num_frames):
        collage = Image.new(
            "RGBA", (width * 2 + 10, height * 2 + 50), (255, 255, 255, 255)
        )
        positions = [
            (10, 0),
            (width + 10, 0),
            (10, height + 10),
            (width + 10, height + 10),
        ]

        for gif, name, pos in zip(gifs, gif_names, positions):
            frame = gif[frame_index]  # Directly access the frame from the list
            gif_width, gif_height = frame.size
            centered_pos = (pos[0], pos[1] + (height - gif_height) // 2)
            collage.paste(frame, centered_pos)
            draw = ImageDraw.Draw(collage)
            # font = ImageFont.load_default()
            font = ImageFont.truetype("Arial.ttf", 16)
            text_width = draw.textlength(name, font=font)
            _, _, text_width, text_height = draw.textbbox((0, 0), text=name, font=font)
            text_position = (
                centered_pos[0] + (gif_width - text_width) // 2,
                centered_pos[1] - text_height - 5,
            )
            draw.text(text_position, name, font=font, fill=(0, 0, 0))

        collage_frames.append(np.array(collage.convert("RGB")))

    video_clip = ImageSequenceClip(collage_frames, fps=10)
    video_clip.write_videofile(f"{folder_path}/collage.mp4", codec="libx264")


def main(folder_path):
    config_path = f"{folder_path}/config.csv"
    config = pd.read_csv(config_path)
    gifs = []
    gif_names = []
    for _, row in config.iterrows():
        frames = trim_gif(f"{folder_path}/{row['Path']}", row["Start"], row["End"])
        gifs.append(frames)  # Append the entire list of frames
        gif_names.append(row["Name"])

    if len(gifs) == 4:
        first_gif = Image.open(f"{folder_path}/{config.iloc[0]['Path']}")
        width, height = first_gif.size
        create_collage(gifs, gif_names, width + 20, height + 50)


if __name__ == "__main__":
    # folder_path = sys.argv[1]
    folder_path = "/Users/khxsh/Downloads/gifs"
    main(folder_path)
