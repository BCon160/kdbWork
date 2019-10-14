//The question isn't very specific so I'm leaving size as a long, if this was fx data, it would be a float
trade:([] time:"N"$(); sym:`$(); price:"F"$(); size:"J"$());

quote:([] time:"N"$(); sym:`$(); bid:"F"$(); bidSize:"J"$(); ask:"F"$(); askSize:"J"$());

marketSummary:([] time:"N"$(); sym:`$(); minPrice:"F"$(); maxPrice:"F"$(); totalTradedVolume:"J"$(); bestBid:"F"$(); bestAsk:"F"$());
