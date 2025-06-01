runtime = 0
runtime = 0
releaseing_skill = ""
skills = {
    ["灵火"] = {key = "5", cd = 10000, press_delay = 100, up_delay = 10, final_release = 0},
    ["缚邪"] = {key = "6", cd = 15000, press_delay = 100, up_delay = 10, final_release = 0},
    ["聚烁"] = {key = "3", cd = 10000, press_delay = 800, up_delay = 10, final_release = 0, charged = true},
    ["灵影"] = {key = "4", cd = 50, press_delay = 50, up_delay = 10, final_release = 0},
    ["祭剑灵元"] = {key = "f2", cd = 50, press_delay = 50, up_delay = 10, final_release = 0},
    ["格挡"] = {key = "e", cd = 6000, press_delay = 2, up_delay = 2, final_release = 0},
}
 
release_skills = {
    {skill_name = "灵火", is_break = true},
    {skill_name = "缚邪", is_break = true},
    {skill_name = "聚烁", is_break = true},
    {skill_name = "灵影", is_break = false},
    {skill_name = "祭剑灵元", is_break = true},
    -- 注意：这里没有添加 on_release 函数，因为您的原始脚本中也没有
}

is_loop = false



function OnEvent(event, arg)
	if (event == "MOUSE_BUTTON_PRESSED" and arg == 4) then
		is_loop = true
		while(is_loop) do
			checkAndRelease()
			Sleep(50)
		end
	end
	if (event == "MOUSE_BUTTON_RELEASED" and arg == 4) then
		is_loop = false
	end

	if (event == "MOUSE_BUTTON_PRESSED" and arg == 4) then
		is_loop = true
		if releaseing_skill ~= ""  and skills[releaseing_skill].charged  and not isCooling("格挡") then
			PressAndReleaseKey("lshift")
		else
			PressAndReleaseKey(skills["格挡"].key)
		end
	end
end


function releaseSkill(skill_name, skill) 
	releaseing_skill = skill_name
	skill.final_release = GetRunningTime() 
	PressKey(skill.key)
	OutputLogMessage("releaseing_skill: "..skill_name.."\n")
	Sleep(skill.press_delay)
	releaseing_skill = ""
	ReleaseKey(skill.key)
	Sleep(skill.up_delay)
end

function checkAndRelease()
	for index, value in ipairs(release_skills) do
	    local skill = skills[value.skill_name]
	    if isCooling(value.skill_name)  then
	    	releaseSkill(value.skill_name, skill)
	    	if value.on_release then
	    		value.on_release()
	    	end
	    	if value.is_break then
	    		break
	    	end
	    end
	end
end

function isCooling(skill_name)
	skill = skills[skill_name]
	return (GetRunningTime() - skill.final_release) >= skill.cd
end