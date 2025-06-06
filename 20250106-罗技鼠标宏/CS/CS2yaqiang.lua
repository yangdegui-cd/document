-- WeaponMaps is a table that maps weapon names to their respective recoil patterns

dofile("/Users/yangdegui/Nextcloud/document/20250106-罗技鼠标宏/CS/弹道.lua")

if (WeaponMaps ~= nil) then
	OutputLogMessage("WeaponMaps loaded successfully.\n")
else
	OutputLogMessage("Failed to load WeaponMaps.\n")
end

-- when the mouse button {offkey} is released, 
-- reset activeWeapon to nil or set activeWeapon to KeySet's value based on the last click
local offkey = 5

local round = 30 -- Number of rounds to fire
local speed = 2
local wireless = 1

local currentTime = 0
local lastPressTime = 0
local clickCount = 1
local lastClickKey = nil
local clickDelay = 200 -- Delay in milliseconds for double click detection
local activeWeapon = nil


-- bullet cursor is used to track the current bullet in the recoil patterns
local bulletCursor = 1
local backx = 0
local backy = 0
local backx1 = 0.00
local backy1 = 0.00
local backx2 = 0
local backy2 = 0
local backx3 = 0
local backy3 = 0
local tsleep = 0.00
local tsleep2 = 0
local tsleep3 = 0.00
local i = 0
local canFire = 1
local timestart = 0

-- when the keyboard button {ShieldCode} is pressed, not to move the mouse
local ShieldCode = "lalt"


-- when the mouse button is pressed, if button is KeySet's key, set activeWeapon to KeySet's value
-- click 1 times to set activeWeapon to KeySet's first value, click 2 times to set activeWeapon to KeySet's second value, etc.
-- click delay is {clickDelay} milliseconds

local KeySet = {
	[4] = {"ak47", "galil", "mac10"},
	[3] = {"m4a1", "m4a4", "famas", "ump45"},
	[6] = {"aug", "mp7", "mp9"},
	[7] = {"awp", "sg556", "g3sg1"},
	[8] = {"p90", "nova", "xm1014"},
}


EnablePrimaryMouseButtonEvents(true)

function OnEvent(event, arg)
	if (wireless) then
		Sleep(1)
	end
	while IsMouseButtonPressed(1) and canFire == 1 do
		if (WeaponMaps[activeWeapon] ~= nil and not IsModifierPressed(ShieldCode)) then
			Weapon = WeaponMaps[activeWeapon]
			if bulletCursor < #Weapon then
				if bulletCursor == 1 then
					timestart = GetRunningTime()
				end		
				MoveMouseRelative(Weapon[bulletCursor].x, Weapon[bulletCursor].y)
				backx = backx - Weapon[bulletCursor].x
				backy = backy - Weapon[bulletCursor].y
				timestart = timestart + Weapon[bulletCursor].d
				SleepToTime(timestart)
				bulletCursor = bulletCursor + 1
			else
				backx2 = math.floor(backx / 40)
				backy2 = math.floor(backy / 40)
				i = 0
				while (i < round) do
					tsleep = 2* math.sqrt(math.abs(backx) * math.abs(backx) + math.abs(backy) * math.abs(backy)) / round / speed
					tsleep2 = math.floor(tsleep)
					tsleep3 = tsleep3 + tsleep - tsleep2
					if tsleep3 >= 1 then
						tsleep3 = tsleep3 - 1
						tsleep2 = tsleep2 + 1
					end
					SleepTime(tsleep2)
					backx1 = backx1 + backx / 40 - backx2
					backy1 = backy1 + backy / 40 - backy2
					if (backx1 >= 1) then
						backx1 = backx1 - 1
						backx3 = backx2 + 1
					else
						backx3 = backx2
					end
					if (backy1 >= 1) then
						backy1 = backy1 - 1
						backy3 = backy2 + 1
					else
						backy3 = backy2
					end
					MoveMouseRelative(backx3, backy3)
					i = i + 1
				end
				initializeState(0)
				Sleep(1000)
			end
		end
	end
	if event == "MOUSE_BUTTON_PRESSED" then
		if (arg == offkey) then
			if activeWeapon ~= nil then
				activeWeapon = nil
				initializeState(1)
				OutputLogMessage("Deactivating weapon: %s\n", activeWeapon)
			elseif (lastClickKey ~= nil and KeySet[lastClickKey] ~= nil) then
				activeWeapon = KeySet[lastClickKey][clickCount]
				initializeState(1)
				OutputLogMessage("Active weapon set to: %s\n", activeWeapon)
			end
		else
			if (KeySet[arg] ~= nil)then
				currentTime = GetRunningTime()
				if (currentTime - lastPressTime < clickDelay and lastClickKey == arg and clickCount < #KeySet[arg]) then
					clickCount = clickCount + 1
				else
					lastClickKey = arg
					clickCount = 1
				end
				lastPressTime = currentTime
				activeWeapon = KeySet[arg][clickCount]
				initializeState(1)
				OutputLogMessage("Active weapon set to: %s\n", activeWeapon)
			end
		end
    end

	if (event == "MOUSE_BUTTON_RELEASED" and arg == 1) then
		initializeState(1)
	end
end

function initializeState(_firing)
	bulletCursor = 1
	backx = 0
	backy = 0
	backx1 = 0.00
	backy1 = 0.00
	tsleep3 = 0.00
	canFire = _firing
end

function SleepTime(time)
	start = GetRunningTime()
	while (time + start > GetRunningTime())
	do
	end
end

function SleepToTime(time)
	while (time > GetRunningTime())
	do
	end
end