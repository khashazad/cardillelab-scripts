import os
import pandas as pd
from PIL import Image, ImageSequence, ImageDraw, ImageFont
from moviepy.editor import (
    ImageSequenceClip,
    VideoFileClip,
)
import numpy as np

fps = 3


def trim_gif(path, start_frame, end_frame):
    gif = Image.open(path)
    frames = [frame.copy() for frame in ImageSequence.Iterator(gif)]
    if start_frame and end_frame:
        return frames[start_frame : end_frame + 1]
    return frames


def create_collage(gifs, gif_names, width, height, output_path):
    collage_frames = []
    num_frames = min(
        len(gif) for gif in gifs
    )  # Correctly calculate the minimum number of frames

    for frame_index in range(num_frames):
        collage = Image.new(
            # "RGBA", (width * 2 + 10, height * 2 + 50), (255, 255, 255, 255)
            "RGB",
            (width * 2 + 10, height * 2 + 50),
            (27, 33, 44),
        )
        positions = [
            (10, 0),
            (width + 10, 0),
            (10, height + 10),
            (width + 10, height + 10),
        ]

        for gif, name, pos in zip(gifs, gif_names, positions):
            frame = gif[frame_index]
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
            draw.text(text_position, name, font=font, fill=(255, 255, 255))

        collage_frames.append(np.array(collage.convert("RGB")))

    video_clip = ImageSequenceClip(collage_frames, fps=fps)
    if os.path.exists(output_path):
        os.remove(output_path)
    video_clip.write_videofile(output_path, codec="libx264")


def main(folder_path):
    config_path = f"{folder_path}/config.csv"
    config = pd.read_csv(config_path)
    gifs = []
    gif_names = []
    for _, row in config.iterrows():
        start_frame = row["Start"] if "Start" in row else None
        end_frame = row["End"] if "End" in row else None
        frames = trim_gif(f"{folder_path}/{row['Path']}", start_frame, end_frame)
        gifs.append(frames)
        gif_names.append(row["Name"])

    if len(gifs) == 4:
        first_gif = Image.open(f"{folder_path}/{config.iloc[0]['Path']}")
        width, height = first_gif.size
        width += 20
        height += 50

        output_path = folder_path + "/collage.mp4"

        create_collage(gifs, gif_names, width, height, output_path)

        output_gif_path = folder_path + "/collage.gif"
        clip = VideoFileClip(output_path)
        clip.write_gif(output_gif_path, fps=fps)


if __name__ == "__main__":
    # folder_path = sys.argv[1]
    root_path = "/Users/khxsh/Desktop/gifs-v2/point 4"

    for folder_path in os.listdir(root_path):
        subfolder_path = os.path.join(root_path, folder_path)
        print(subfolder_path)
        if os.path.isdir(subfolder_path):
            print(subfolder_path)
            main(subfolder_path)
