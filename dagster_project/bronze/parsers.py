import requests
from bs4 import BeautifulSoup
from datetime import datetime
from typing import Dict, List, Optional
from shared.constants import HEADERS


class InsideAirbnbParser:
    def __init__(self, source_url: str):
        self.source_url = source_url
        self._soup: Optional[BeautifulSoup] = None

    def _get_soup(self) -> BeautifulSoup:
        if self._soup is None:
            response = requests.get(self.source_url, timeout=30, headers=HEADERS)
            html_content = response.content.decode("utf-8", errors="replace")
            self._soup = BeautifulSoup(html_content, "html.parser")
        return self._soup

    def get_all_partitions(self) -> List[Dict[str, str]]:
        """Extract all city, country, date combinations from the website."""
        soup = self._get_soup()
        partitions = []
        h3s = soup.find_all("h3")

        for h3 in h3s:
            h3_text = h3.get_text(strip=True)
            if "," in h3_text:
                city, country = [s.strip() for s in h3_text.split(",", 1)]

                h4 = h3.find_next_sibling("h4")
                while h4 and h4.name != "h3":
                    if h4.name == "h4":
                        date_text = h4.get_text(strip=True).split("(")[0].strip()
                        try:
                            # Convert "14 September, 2025" to "2025-09-14"
                            iso_date = datetime.strptime(
                                date_text, "%d %B, %Y"
                            ).strftime("%Y-%m-%d")
                            partitions.append(
                                {"city": city, "country": country, "date": iso_date}
                            )
                        except ValueError:
                            pass
                    h4 = h4.find_next_sibling()
        return partitions

    def get_urls_for_partition(
        self, city: str, country: str, date: str
    ) -> Dict[str, str]:
        """Extract download URLs for a specific partition."""
        soup = self._get_soup()
        target_urls = {}
        h3s = soup.find_all("h3")

        targets = [
            "listings.csv",
            "reviews.csv",
            "neighbourhoods.csv",
            "neighbourhoods.geojson",
        ]

        for h3 in h3s:
            h3_text = h3.get_text()
            if city in h3_text and country in h3_text:
                h4 = h3.find_next_sibling("h4")
                while h4 and h4.name != "h3":
                    if h4.name == "h4":
                        h4_text = h4.get_text(strip=True).split("(")[0].strip()
                        try:
                            h4_date = datetime.strptime(h4_text, "%d %B, %Y").strftime(
                                "%Y-%m-%d"
                            )
                            if h4_date == date:
                                table = h4.find_next_sibling("table")
                                if table:
                                    rows = table.find_all("tr")
                                    for row in rows:
                                        link = row.find("a")
                                        if link and "href" in link.attrs:
                                            href = link["href"]
                                            for t in targets:
                                                if href.endswith(t) and (
                                                    t.endswith(".gz")
                                                    or not href.endswith(".gz")
                                                ):
                                                    if t.endswith(
                                                        ".csv"
                                                    ) and href.endswith(".csv.gz"):
                                                        continue
                                                    target_urls[t] = href
                                return target_urls
                        except ValueError:
                            pass
                    h4 = h4.find_next_sibling()
        return target_urls
