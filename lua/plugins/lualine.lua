local colors = {
    grey = "#4a4a4a",
    dark_grey = "#2e2e2e",
    white = "#f3f3f3"
}

return {
    'nvim-lualine/lualine.nvim',
    dependencies = { 'nvim-tree/nvim-web-devicons' },
    config = function()
        local lualine = require('lualine')

        local theme = require('lualine.themes.auto')

        theme.normal.c.bg = 'none'
        theme.normal.b = { bg = colors.grey, fg = colors.white }
        theme.normal.y = { bg = colors.grey, fg = colors.white }

        local empty = require('lualine.component'):extend()
        function empty:draw(default_highlight)
            self.status = ''
            self.applied_separator = ''
            self:apply_highlights(default_highlight)
            self:apply_section_separators()
            return self.status
        end

        -- Put proper separators and gaps between components in sections
        local function process_sections(sections)
            for name, section in pairs(sections) do
                local left = name:sub(9, 10) < 'x'
                for pos = 1, name ~= 'lualine_z' and #section or #section - 1 do
                    table.insert(section, pos * 2, { empty, color = { fg = colors.dark_grey, bg = colors.dark_grey } })
                end
                for id, comp in ipairs(section) do
                    if type(comp) ~= 'table' then
                        comp = { comp }
                        section[id] = comp
                    end
                    comp.separator = left and { right = '' } or { left = '' }
                end
            end
            return sections
        end

        require('lualine').setup {
            options = {
                theme = theme,
                component_separators = '',
                section_separators = { left = '', right = '' },
            },
            sections = process_sections {
                lualine_a = { 'mode' },
                lualine_b = {
                    'branch',
                    { 'filename', file_status = false, path = 1 },
                },
                lualine_c = {},
                lualine_x = {},
                lualine_y = { 'filetype' },
                lualine_z = { '%l:%c', '%p%%/%L' },
            },
            inactive_sections = {
                lualine_c = { '%f %y %m' },
                lualine_x = {},
            },
        }
    end
}
