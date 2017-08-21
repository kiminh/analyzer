

function parseV1(timesec, interests, user, device, network, loc, media, ad)
    local i = 0
    local elements = {}

    local w = os.date("%w", timesec)
    table.insert(elements, i + w)
    i = i + 7

    local hour = os.date("%H", timesec)
    table.insert(elements, i + hour)
    i = i + 24

    table.sort(interests)
    for k, v in ipairs(interests) do
        idx = interest[v]
        if (idx ~= nil and idx > 0) then
            ---table.insert(elements, i + idx)
        end
    end
    i = i + 200

    table.insert(elements, i + user.sex)
    i = i + 10

    local age = 0
    if (user.age <= 1) then
        age = 1
    elseif (user.age <= 4) then
        age = 2
    else
        age = 3
    end
    table.insert(elements, i + age)
    i = i + 100

    table.insert(elements, i + device.os)
    i = i + 10

    table.insert(elements, i + device.phoneLevel)
    i = i + 10

    table.insert(elements, i + network.isp)
    i = i + 20

    table.insert(elements, i + network.nettype)
    i = i + 10

    local cid = city[loc.city]
    table.insert(elements,i + (cid ~= nil and cid or 0))
    i = i + 1000

    local slotid = adslot[media.slotid]
    table.insert(elements,i + (slotid ~= nil and slotid or 0))
    i = i + 1000

    table.insert(elements,i + media.slottype)
    i = i + 10

    local chnl = channel[media.channel]
    table.insert(elements,i + (chnl ~= nil and chnl or 0))
    i = i + 200

    local adc = adclass[ad.adclass]
    table.insert(elements,i + (adc ~= nil and adc or 0))
    i = i + 1000

    table.insert(elements,i + ad.adtype)
    i = i + 10

    local adid = 0
    if (ad.id >= 1500000) then
        adid = ad.id - 1500000 + 60000
    elseif (ad.id >= 1000000) then
        adid = ad.id - 1000000 + 30000
    elseif (ad.id < 30000 and ad.id > 0) then
        adid = ad.id
    end
    table.insert(elements,i + adid)
    table.insert(elements,i + 200000)

    return table.concat(elements, " ")
end



