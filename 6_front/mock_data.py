import datetime

MOCK_ADS = [
    {
        "id": 1,
        "product_name": "Smartwatch Pro",
        "platform": "Facebook",
        "publication_date": datetime.date(2024, 3, 15),
        "image_url": "https://via.placeholder.com/150/FF0000/FFFFFF?text=Smartwatch",
        "likes": 1500,
        "comments": 300,
        "shares": 500,
        "reactions": 2300, # likes + comments + shares for simplicity
        "country": "USA",
        "language": "English",
        "tags": ["🔥 Trending", "Gadget"]
    },
    {
        "id": 2,
        "product_name": "Wireless Earbuds",
        "platform": "TikTok",
        "publication_date": datetime.date(2024, 4, 1),
        "image_url": "https://via.placeholder.com/150/00FF00/FFFFFF?text=Earbuds",
        "likes": 5000,
        "comments": 1200,
        "shares": 2000,
        "reactions": 8200,
        "country": "France",
        "language": "French",
        "tags": ["🎧 Audio", "🏆 Best of the month"]
    },
    {
        "id": 3,
        "product_name": "Yoga Mat",
        "platform": "Pinterest",
        "publication_date": datetime.date(2024, 3, 20),
        "image_url": "https://via.placeholder.com/150/0000FF/FFFFFF?text=Yoga+Mat",
        "likes": 800,
        "comments": 150,
        "shares": 300,
        "reactions": 1250,
        "country": "Germany",
        "language": "German",
        "tags": ["🧘‍♀️ Wellness"]
    },
    {
        "id": 4,
        "product_name": "Portable Blender",
        "platform": "Facebook",
        "publication_date": datetime.date(2024, 4, 5),
        "image_url": "https://via.placeholder.com/150/FFFF00/000000?text=Blender",
        "likes": 2200,
        "comments": 450,
        "shares": 700,
        "reactions": 3350,
        "country": "USA",
        "language": "English",
        "tags": ["🔥 Trending", "Kitchen"]
    },
    {
        "id": 5,
        "product_name": "Gaming Chair",
        "platform": "TikTok",
        "publication_date": datetime.date(2024, 3, 10),
        "image_url": "https://via.placeholder.com/150/FF00FF/FFFFFF?text=Gaming+Chair",
        "likes": 10000,
        "comments": 2500,
        "shares": 4000,
        "reactions": 16500,
        "country": "UK",
        "language": "English",
        "tags": ["🎮 Gaming", "🏆 Best of the month"]
    },
     {
        "id": 6,
        "product_name": "Smartwatch Lite",
        "platform": "Facebook",
        "publication_date": datetime.date(2024, 4, 8),
        "image_url": "https://via.placeholder.com/150/FFA500/FFFFFF?text=Smartwatch+Lite",
        "likes": 900,
        "comments": 180,
        "shares": 250,
        "reactions": 1330,
        "country": "Canada",
        "language": "English",
        "tags": ["Gadget"]
    },
    {
        "id": 7,
        "product_name": "Noise Cancelling Headphones",
        "platform": "Pinterest",
        "publication_date": datetime.date(2024, 3, 25),
        "image_url": "https://via.placeholder.com/150/008080/FFFFFF?text=Headphones",
        "likes": 3500,
        "comments": 700,
        "shares": 1000,
        "reactions": 5200,
        "country": "Australia",
        "language": "English",
        "tags": ["🎧 Audio", "🔥 Trending"]
    },
    {
        "id": 8,
        "product_name": "Resistance Bands Set",
        "platform": "TikTok",
        "publication_date": datetime.date(2024, 4, 2),
        "image_url": "https://via.placeholder.com/150/800080/FFFFFF?text=Resistance+Bands",
        "likes": 6000,
        "comments": 1500,
        "shares": 2500,
        "reactions": 10000,
        "country": "USA",
        "language": "English",
        "tags": ["💪 Fitness", "🏆 Best of the month"]
    }
]

def get_mock_ads():
    """Returns the list of mock ad data."""
    return MOCK_ADS
