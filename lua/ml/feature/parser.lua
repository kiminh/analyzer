



function parser(ad, user, media, net, device, loc, timesec)
    i = 0
    elements = {}
    t = os.date("*t", timesec)

    elements[i + t.wday] = 1
    i = i + 7

    elements[i + t.hour] = 1
    i = i + 24

    for v in pairs(user.interests) do

    end

    i = i + 200

    elements[i + user.sex] = 1
    i = i + 10

end


t = os.date("*t", 0)

return t.wday
