from Parsing.ParsingStrategy import ParsingStrategy


class Sentinel2ParsingStrategy(ParsingStrategy):
    def extract_image_record(self, observation):
        return {
            "img_id": observation[19],
            "id_doy": observation[17],
            "img_date": observation[18],
            "img_day": observation[19],
            "img_month": observation[20],
            "img_year": observation[22],
            "cloud_cover": observation[14],
            "cloud_cover_land": observation[15],
        }

    def extract_record(self, observation):
        return {
            "system_index": observation[0],
            "hylak_id": observation[2],
            "img_id": observation[19],
            "qa_pixel": float(observation[3]),
            "qa_radsat": float(observation[4]),
            "sr_band1": float(observation[5]),
            "sr_band2": float(observation[6]),
            "sr_band3": float(observation[7]),
            "sr_band4": float(observation[8]),
            "sr_band5": float(observation[9]),
            "sr_band6": float(observation[10]),
            "sr_band7": float(observation[11]),
            "sr_qa_aerosol": float(observation[12]),
            "st_band10": float(observation[13]),
        }
