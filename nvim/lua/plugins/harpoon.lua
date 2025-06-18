return {
    "theprimeagen/harpoon",
    branch = "harpoon2",
    dependencies = { "nvim-lua/plenary.nvim", "nvim-telescope/telescope.nvim" },

    config = function()
        local harpoon = require("harpoon")

        harpoon:setup()

        vim.keymap.set("n", "<leader>A", function()
            harpoon:list():prepend()
        end)
        vim.keymap.set("n", "<leader>a", function()
            harpoon:list():add()
        end)
        vim.keymap.set("n", "<C-e>", function()
            harpoon.ui:toggle_quick_menu(harpoon:list())
        end)

        vim.keymap.set("n", "<leader>h1", function()
            harpoon:list():select(1)
        end)
        vim.keymap.set("n", "<leader>h2", function()
            harpoon:list():select(2)
        end)
        vim.keymap.set("n", "<leader>h3", function()
            harpoon:list():select(3)
        end)
        vim.keymap.set("n", "<leader>h4", function()
            harpoon:list():select(4)
        end)
        vim.keymap.set("n", "<leader>hr1", function()
            harpoon:list():replace_at(1)
        end)
        vim.keymap.set("n", "<leader>hr2", function()
            harpoon:list():replace_at(2)
        end)
        vim.keymap.set("n", "<leader>hr3", function()
            harpoon:list():replace_at(3)
        end)
        vim.keymap.set("n", "<leader>hr4", function()
            harpoon:list():replace_at(4)
        end)
        vim.keymap.set("n", "<leader>hp", function()
            harpoon:list():prev()
        end, { noremap = true })
        vim.keymap.set("n", "<leader>hn", function()
            harpoon:list():next()
        end, { noremap = true })
        local conf = require("telescope.config").values
        local function toggle_telescope(harpoon_files)
            local file_paths = {}
            for _, item in ipairs(harpoon_files.items) do
                table.insert(file_paths, item.value)
            end

            require("telescope.pickers").new({}, {
                prompt_title = "Harpoon",
                finder = require("telescope.finders").new_table({
                    results = file_paths,
                }),
                previewer = conf.file_previewer({}),
                sorter = conf.generic_sorter({}),
            }):find()
        end
        vim.keymap.set("n", "<leader>ph", function() toggle_telescope(harpoon:list()) end,
            { desc = "Open harpoon window", noremap = true })
    end
}
