runtime = 0
releaseing_skill = ""
interrupt_releaseing_skill = false
spell_time = 0
spelled_time = 0

skills = {
    ["灵火炙魂"] = {key = "5", cd = 10000, press_delay = 400, up_delay = 50, final_release = 0},
    ["缚邪"] = {key = "6", cd = 15000, press_delay = 500, up_delay = 50, final_release = 0},
    ["聚烁"] = {key = "3", cd = 10000, press_delay = 900, up_delay = 50, final_release = 0, charged = true},
    ["灵引"] = {key = "4", cd = 50, press_delay = 700, up_delay = 50, final_release = 0},
    ["祭剑灵元"] = {key = "f2", cd = 50, press_delay = 500, up_delay = 50, final_release = 0},
    ["律令焚邪"] = {key = "f2", cd = 600000, press_delay = 700, up_delay = 50, final_release = 0},
    ["格挡"] = {key = "e", cd = 6000, press_delay = 50, up_delay = 50, final_release = 0},
    ["炎龙刻印"] = {key = "q", cd=20000, press_delay=500, up_delay=50, final_release = 0},
    ["南巫天火"] = {key = "", cd=10000, press_delay=700, up_delay=50, final_release = 0},
    ["幻火迷觉"] = {key = "", cd=15000, press_delay=500, up_delay=50, final_release = 0},
    ["炎龙刻印"] = {key = "", cd=20000, press_delay=500, up_delay=50, final_release = 0},
    ["驭火诀"] = {key = "", cd=4000, press_delay=300, up_delay=50, final_release = 0},
    ["五龙御焰"] = {key = "", cd=60000, press_delay=1000, up_delay=50, final_release = 0},
    ["意气风发"] = {key = "", cd=60000, press_delay=200, up_delay=50, final_release = 0},
    ["焚秽"] = {key = "", cd=0, press_delay=500, up_delay=50, final_release = 0},
    ["法宝"] = {key = "", cd=60000, press_delay=400, up_delay=50, final_release = 0},
}

xhz = 0

--灵火炙魂减少南巫天火CD天书
lhzh_reduce_cd = true
-- 焚秽减少驭火诀CD天书
fh_reduce_cd = true

-- 炎焚香pve
release_skills = {
	{skill_name = "法宝", is_break=true},
	{skill_name = "律令焚邪", is_break=true},
	{skill_name = "焚秽", is_break=false},
	{skill_name = "祭剑灵元", is_break=false},
	{skill_name = "祭剑灵元", is_break=false, 
		can_release = function() 
			skills["祭剑灵元"].press_delay = 0
			return true
		end,
		after_released = function()
			releaseSkill("灵火炙魂")
			releaseSkill("灵引")
		end},
}

is_loop = false



function OnEvent(event, arg)
	if (event == "MOUSE_BUTTON_PRESSED" and arg == 4) then
		is_loop = true
		while(is_loop) do
			checkAndRelease()
		end
	end
	if (event == "MOUSE_BUTTON_RELEASED" and arg == 4) then
		is_loop = false
		interrupt_releaseing_skill = true
	end

	if (event == "MOUSE_BUTTON_PRESSED" and arg == 2) then
		if  IsKeyLockOn("scrolllock")==false then break end
		is_loop = false
		if spell_time - spelled_time >= 200 or isCooling("格挡") then
			OutputLogMessage("技能施法时间还有: "..spell_time - spelled_time.."ms, 闪避打断\n")
			interrupt_releaseing_skill = true
			PressAndReleaseKey("lshift")
		else
			releaseSkill("格挡")
		end
	end
end


function releaseSkill(skill_name) 
	local skill = skills[skill_name]

	interrupt_releaseing_skill = false
	releaseing_skill = skill_name
	skill.final_release = GetRunningTime() 
	spell_time = skill.press_delay
    spelled_time = 0

	PressKey(skill.key)
	OutputLogMessage("释放技能: "..skill_name.."\n")

	while(spelled_time < spell_time and not interrupt_releaseing_skill) do
		Sleep(50)
		spelled_time += 50
	end

	releaseing_skill = ""
	spell_time = 0
    spelled_time = 0

	ReleaseKey(skill.key)

	if not interrupt_releaseing_skill then
		Sleep(skill.up_delay)
	end
end

function checkAndRelease()
	for index, value in ipairs(release_skills) do
	    local skill = skills[value.skill_name]
	    if not isCooling(value.skill_name) and (not value.can_release or value.can_release()) then
	    	releaseSkill(value.skill_name, skill)
	    	if value.after_released then
	    		value.after_released()
	    	end
	    	if value.is_break then
	    		break
	    	end
	    end
	end
end

function isCooling(skill_name)
	local skill = skills[skill_name]
	return (GetRunningTime() - skill.final_release) < skill.cd
end