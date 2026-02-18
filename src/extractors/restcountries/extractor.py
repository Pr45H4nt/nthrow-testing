from nthrow.source import SimpleSource


class Extractor(SimpleSource):
	def __init__(self, *args, **kwargs):
		super().__init__(*args, **kwargs)

	def make_url(self, row, _type):

		args = self.prepare_request_args(row, _type)


		region = args["q"]["path"]

		fields = "name,capital,population,currencies,languages,flag,cca3"
		return f"https://restcountries.com/v3.1/region/{region}?fields={fields}"

	async def fetch_rows(self, row, _type="to"):

		try:
			url = self.make_url(row, _type)
			res = await self.http_get(url)

			if res.status_code == 200:
				countries = res.json()
				rows = []

				for c in self.clamp_rows_length(countries):
					rows.append(
						self.make_a_row(
							row["uri"],
							
							self.mini_uri(
								f"https://restcountries.com/#{c['cca3']}"
							),
							{
								"name": c["name"]["common"],
								"official_name": c["name"]["official"],
								"capital": c.get("capital", [None])[0],
								"population": c["population"],
								"currencies": list(
									c.get("currencies", {}).keys()
								),
								"languages": list(
									c.get("languages", {}).values()
								),
								"flag": c.get("flag", ""),
							}
						)
					)

				return {
					"rows": rows,
					"state": {
						"pagination": {
							
							_type: None
						}
					}
				}
			else:
				self.logger.error(
					"Non-200 HTTP response: %s : %s" % (res.status_code, url)
				)
				return self.make_error("HTTP", res.status_code, url)
		except Exception as e:
			self.logger.exception(e)
			return self.make_error("Exception", type(e).__name__, str(e))
