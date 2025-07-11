
EnablePrimaryMouseButtonEvents(true)

BINGXI_KEY = "."

function OnEvent(event, arg)
    if event == "MOUSE_BUTTON_PRESSED" and arg == 1 then
        PressKey(BINGXI_KEY)
    end
    if event == "MOUSE_BUTTON_RELEASED" and arg == 1 then
        ReleaseKey(BINGXI_KEY)
    end
end