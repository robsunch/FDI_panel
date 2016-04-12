clear all
gen x1 = .
gen x2 = .

capture confirm variable x1 
local exist_x1 = _rc

if ~_rc {
    disp "x1 exists"
    capture confirm variable x3
    local exist_x3 = _rc
}
else {
    disp _rc
    disp `exist_x1'
}
